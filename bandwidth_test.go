package gossipsubtrace

import (
	"bytes"
	"encoding/binary"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func mixedRPC() *pubsub.RPC {
	return &pubsub.RPC{RPC: pb.RPC{
		Publish: []*pb.Message{{Topic: proto.String("blocks"), Data: bytes.Repeat([]byte{42}, 16384)}},
		Subscriptions: []*pb.RPC_SubOpts{
			{Topicid: proto.String("blocks"), Subscribe: proto.Bool(true)},
			{Topicid: proto.String("old"), Subscribe: proto.Bool(false)},
		},
		Control: &pb.ControlMessage{
			Ihave: []*pb.ControlIHave{
				{TopicID: proto.String("blocks"), MessageIDs: []string{"one", "two"}},
				{TopicID: proto.String("attestations"), MessageIDs: []string{"three"}},
			},
			Iwant:     []*pb.ControlIWant{{MessageIDs: []string{"one", "one"}}},
			Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{"two"}}},
			Graft:     []*pb.ControlGraft{{TopicID: proto.String("blocks")}},
			Prune: []*pb.ControlPrune{{TopicID: proto.String("old"), Backoff: proto.Uint64(60),
				Peers: []*pb.PeerInfo{{PeerID: []byte("peer"), SignedPeerRecord: []byte("record")}}}},
		},
	}}
}

func encoded(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func prefixLen(n int) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], uint64(n))
}

func TestMeasureRPCMatchesSerializedBytes(t *testing.T) {
	extension := mixedRPC()
	extension.Partial = &pb.PartialMessagesExtension{TopicID: proto.String("blocks"), PartialMessage: []byte("partial")}
	extension.Control.Extensions = &pb.ControlExtensions{PartialMessages: proto.Bool(true)}
	unknown := protowire.AppendTag(nil, 1000, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("unknown"))
	extension.ProtoReflect().SetUnknown(unknown)
	extension.Control.ProtoReflect().SetUnknown(unknown)

	tests := map[string]*pubsub.RPC{
		"empty":         {},
		"empty control": {RPC: pb.RPC{Control: &pb.ControlMessage{}}},
		"mixed":         mixedRPC(),
		"extensions":    extension,
		"nil entries": {RPC: pb.RPC{
			Publish: []*pb.Message{nil}, Subscriptions: []*pb.RPC_SubOpts{nil},
			Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{nil}, Iwant: []*pb.ControlIWant{nil}},
		}},
	}
	for name, rpc := range tests {
		t.Run(name, func(t *testing.T) {
			before := encoded(t, &rpc.RPC)
			got := measureRPC(rpc)
			want := uint64(len(before) + prefixLen(len(before)))
			if got.bytes != want {
				t.Fatalf("total bytes = %d, serialized frame = %d", got.bytes, want)
			}

			// Independently parse the serialized fields to check attribution.
			var expected [kindCount]uint64
			expected[overhead] = uint64(prefixLen(len(before)))
			visitFields(t, before, func(num protowire.Number, field, value []byte) {
				k := other
				switch num {
				case 1:
					var sub pb.RPC_SubOpts
					if err := proto.Unmarshal(value, &sub); err != nil {
						t.Fatal(err)
					}
					k = unsubscribe
					if sub.GetSubscribe() {
						k = subscribe
					}
				case 2:
					k = publish
				case 10:
					k = partial
				case 3:
					expected[overhead] += uint64(len(field) - len(value))
					visitFields(t, value, func(num protowire.Number, field, _ []byte) {
						k := other
						switch num {
						case 1:
							k = ihave
						case 2:
							k = iwant
						case 3:
							k = graft
						case 4:
							k = prune
						case 5:
							k = idontwant
						}
						expected[k] += uint64(len(field))
					})
					return
				}
				expected[k] += uint64(len(field))
			})
			if got.kindBytes != expected {
				t.Fatalf("breakdown = %v, serialized fields = %v", got.kindBytes, expected)
			}
			var sum uint64
			for _, n := range got.kindBytes {
				sum += n
			}
			if sum != want {
				t.Fatalf("breakdown sum = %d, total = %d", sum, want)
			}
			if !bytes.Equal(before, encoded(t, &rpc.RPC)) {
				t.Fatal("tracer modified the RPC")
			}
		})
	}
}

func visitFields(t *testing.T, data []byte, visit func(protowire.Number, []byte, []byte)) {
	t.Helper()
	for len(data) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 || typ != protowire.BytesType {
			t.Fatal("expected a length-delimited field")
		}
		value, n := protowire.ConsumeBytes(data[tagLen:])
		if n < 0 {
			t.Fatal("invalid length-delimited field")
		}
		visit(num, data[:tagLen+n], value)
		data = data[tagLen+n:]
	}
}

func partialRPC() *pubsub.RPC {
	return &pubsub.RPC{RPC: pb.RPC{Partial: &pb.PartialMessagesExtension{
		TopicID: proto.String("blocks"), GroupID: []byte("group"),
		PartialMessage: bytes.Repeat([]byte{1}, 300), PartsMetadata: []byte("meta"),
	}}}
}

func TestMeasurePartial(t *testing.T) {
	rpc := partialRPC()
	got := measureRPC(rpc)
	field := encoded(t, rpc.Partial)
	if want := uint64(1 + prefixLen(len(field)) + len(field)); got.kindBytes[partial] != want {
		t.Fatalf("partial bytes = %d, want %d", got.kindBytes[partial], want)
	}
	if want := (pmBytes{message: 300, metadata: 4, header: 11}); got.pm != want {
		t.Fatalf("pm = %+v, want %+v", got.pm, want)
	}
	if got.messages[partial] != 1 || got.kindBytes[other] != 0 {
		t.Fatalf("partial messages = %d, other bytes = %d", got.messages[partial], got.kindBytes[other])
	}
}

func TestMeasureControlCounts(t *testing.T) {
	got := measureRPC(mixedRPC())
	if got.messages[ihave] != 2 || got.messageIDs[ihave] != 3 {
		t.Fatalf("IHAVE entries/IDs = %d/%d", got.messages[ihave], got.messageIDs[ihave])
	}
	if got.messages[iwant] != 1 || got.messageIDs[iwant] != 2 {
		t.Fatalf("IWANT entries/IDs = %d/%d", got.messages[iwant], got.messageIDs[iwant])
	}
	if got.messages[idontwant] != 1 || got.messageIDs[idontwant] != 1 {
		t.Fatalf("IDONTWANT entries/IDs = %d/%d", got.messages[idontwant], got.messageIDs[idontwant])
	}
	for _, k := range []kind{publish, subscribe, unsubscribe, graft, prune} {
		if got.messages[k] != 1 {
			t.Errorf("%s messages = %d, want 1", kindNames[k], got.messages[k])
		}
	}
}
