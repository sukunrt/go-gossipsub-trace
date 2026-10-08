package gossipsubtrace

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type kind int

const (
	publish kind = iota
	subscribe
	unsubscribe
	ihave
	iwant
	idontwant
	graft
	prune
	partial
	overhead
	other
	kindCount
)

var kindNames = [kindCount]string{
	"publish", "subscribe", "unsubscribe", "ihave", "iwant", "idontwant",
	"graft", "prune", "partial", "overhead", "other",
}

// pmBytes holds raw partial-message field values, without protobuf framing.
// The encoded extension size is the partial kind's byte count.
type pmBytes struct{ message, metadata, header uint64 }

func measurePartial(p *pb.PartialMessagesExtension) pmBytes {
	return pmBytes{
		message:  uint64(len(p.GetPartialMessage())),
		metadata: uint64(len(p.GetPartsMetadata())),
		header:   uint64(len(p.GetTopicID()) + len(p.GetGroupID())),
	}
}

func (p *pmBytes) add(q pmBytes) {
	p.message += q.message
	p.metadata += q.metadata
	p.header += q.header
}

func (p pmBytes) minus(q pmBytes) pmBytes {
	return pmBytes{p.message - q.message, p.metadata - q.metadata, p.header - q.header}
}

func encodedFieldSize(field protowire.Number, message proto.Message) uint64 {
	return uint64(protowire.SizeTag(field) + protowire.SizeBytes(proto.Size(message)))
}

// measureRPC counts each known message together with its enclosing protobuf
// field tag and length. Shared control-envelope framing and the RPC length
// prefix go to overhead. The partial-message extension goes to partial. Other
// extensions and unknown fields go to other, preserving the invariant that the
// breakdown sums to the complete encoded RPC size.
func measureRPC(rpc *pubsub.RPC) counters {
	var c counters
	size := proto.Size(&rpc.RPC)
	c.bytes = uint64(protowire.SizeBytes(size))
	c.kindBytes[overhead] = uint64(protowire.SizeVarint(uint64(size)))
	add := func(k kind, field protowire.Number, message proto.Message) {
		c.kindBytes[k] += encodedFieldSize(field, message)
		c.messages[k]++
	}
	for _, message := range rpc.Publish {
		add(publish, 2, message)
	}
	for _, sub := range rpc.Subscriptions {
		k := unsubscribe
		if sub.GetSubscribe() {
			k = subscribe
		}
		add(k, 1, sub)
	}
	if control := rpc.Control; control != nil {
		c.kindBytes[overhead] += uint64(protowire.SizeTag(3) + protowire.SizeVarint(uint64(proto.Size(control))))
		for _, message := range control.Ihave {
			add(ihave, 1, message)
			c.messageIDs[ihave] += uint64(len(message.GetMessageIDs()))
		}
		for _, message := range control.Iwant {
			add(iwant, 2, message)
			c.messageIDs[iwant] += uint64(len(message.GetMessageIDs()))
		}
		for _, message := range control.Graft {
			add(graft, 3, message)
		}
		for _, message := range control.Prune {
			add(prune, 4, message)
		}
		for _, message := range control.Idontwant {
			add(idontwant, 5, message)
			c.messageIDs[idontwant] += uint64(len(message.GetMessageIDs()))
		}
	}
	if p := rpc.Partial; p != nil {
		add(partial, 10, p)
		c.pm = measurePartial(p)
	}
	var attributed uint64
	for _, bytes := range c.kindBytes {
		attributed += bytes
	}
	c.kindBytes[other] = c.bytes - attributed
	return c
}
