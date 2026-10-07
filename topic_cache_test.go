package gossipsubtrace

import (
	"testing"
	"time"
)

func TestTopicCacheExpiration(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("id", "blocks", now)
	if got := cache.topic("id", now.Add(MessageIDRetention-time.Nanosecond)); got != "blocks" {
		t.Fatalf("topic before expiration = %q, want blocks", got)
	}
	// Looking up an ID does not extend its retention period.
	if got := cache.topic("id", now.Add(MessageIDRetention)); got != "" {
		t.Fatalf("topic at expiration = %q, want unknown", got)
	}
	if len(cache.entries) != 0 {
		t.Fatal("lookup retained an expired entry")
	}
	if cache.expiry.Len() != 0 {
		t.Fatal("lookup retained an expired list node")
	}
}

func TestTopicCacheRefresh(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("id", "blocks", now)
	refreshed := now.Add(time.Minute)
	cache.remember("id", "blocks", refreshed)
	if got := cache.topic("id", now.Add(MessageIDRetention)); got != "blocks" {
		t.Fatalf("topic after original expiration = %q, want blocks", got)
	}
	if got := cache.topic("id", refreshed.Add(MessageIDRetention)); got != "" {
		t.Fatalf("topic at refreshed expiration = %q, want unknown", got)
	}
}

func TestTopicCacheCleanup(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("old", "blocks", now)
	cache.remember("recent", "attestations", now.Add(time.Minute))
	cache.cleanup(now.Add(MessageIDRetention))
	if len(cache.entries) != 1 {
		t.Fatalf("entries after cleanup = %d, want 1", len(cache.entries))
	}
	if cache.expiry.Len() != 1 {
		t.Fatalf("list nodes after cleanup = %d, want 1", cache.expiry.Len())
	}
	if got := cache.topic("recent", now.Add(MessageIDRetention)); got != "attestations" {
		t.Fatalf("remaining topic = %q, want attestations", got)
	}
	cache.cleanup(now.Add(2 * MessageIDRetention))
	if len(cache.entries) != 0 {
		t.Fatal("cleanup retained expired entries")
	}
	if cache.expiry.Len() != 0 {
		t.Fatal("cleanup retained expired list nodes")
	}
}

func TestTopicCacheRefreshReordersExpiration(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("first", "blocks", now)
	cache.remember("second", "attestations", now.Add(time.Second))
	cache.remember("first", "blocks", now.Add(time.Minute))
	if got := cache.expiry.Front().Value.(*topicEntry).id; got != "second" {
		t.Fatalf("oldest entry after refresh = %q, want second", got)
	}
	if got := cache.expiry.Back().Value.(*topicEntry).id; got != "first" {
		t.Fatalf("newest entry after refresh = %q, want first", got)
	}
	cache.cleanup(now.Add(MessageIDRetention + time.Second))
	if _, exists := cache.entries["second"]; exists {
		t.Fatal("expired entry survived cleanup behind a refreshed entry")
	}
	if got := cache.topic("first", now.Add(MessageIDRetention+time.Second)); got != "blocks" {
		t.Fatalf("refreshed topic after cleanup = %q, want blocks", got)
	}
	if len(cache.entries) != 1 || cache.expiry.Len() != 1 {
		t.Fatalf("cache size after cleanup: map=%d list=%d, want 1 each", len(cache.entries), cache.expiry.Len())
	}
}

func TestTopicCacheRepeatedRefreshReusesNode(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("id", "blocks", now)
	element := cache.entries["id"]
	for i := 1; i <= 1000; i++ {
		cache.remember("id", "blocks", now.Add(time.Duration(i)*time.Millisecond))
	}
	if len(cache.entries) != 1 || cache.expiry.Len() != 1 {
		t.Fatalf("cache size after refreshes: map=%d list=%d, want 1 each", len(cache.entries), cache.expiry.Len())
	}
	if cache.entries["id"] != element {
		t.Fatal("refresh replaced the existing list node")
	}
}

func TestTopicCacheExpiredLookupRemovesMiddleNode(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("first", "blocks", now)
	cache.remember("middle", "blocks", now.Add(time.Second))
	cache.remember("last", "attestations", now.Add(time.Minute))
	if got := cache.topic("middle", now.Add(MessageIDRetention+time.Second)); got != "" {
		t.Fatalf("expired middle topic = %q, want unknown", got)
	}
	if _, exists := cache.entries["middle"]; exists {
		t.Fatal("lookup retained an expired middle map entry")
	}
	if len(cache.entries) != 2 || cache.expiry.Len() != 2 {
		t.Fatalf("cache size after lookup: map=%d list=%d, want 2 each", len(cache.entries), cache.expiry.Len())
	}
	cache.cleanup(now.Add(MessageIDRetention + time.Second))
	if len(cache.entries) != 1 || cache.expiry.Len() != 1 {
		t.Fatalf("cache size after cleanup: map=%d list=%d, want 1 each", len(cache.entries), cache.expiry.Len())
	}
	if got := cache.topic("last", now.Add(MessageIDRetention+time.Second)); got != "attestations" {
		t.Fatalf("remaining topic = %q, want attestations", got)
	}
}

func TestTopicCacheAmbiguity(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("id", "blocks", now)
	cache.remember("id", "attestations", now.Add(time.Second))
	if got := cache.topic("id", now.Add(time.Second)); got != "" {
		t.Fatalf("conflicting topic = %q, want unknown", got)
	}
	// Repeated advertisements must not silently resolve conflicting claims.
	refreshed := now.Add(time.Minute)
	cache.remember("id", "blocks", refreshed)
	if got := cache.topic("id", refreshed); got != "" {
		t.Fatalf("repeated conflicting topic = %q, want unknown", got)
	}
	cache.cleanup(now.Add(MessageIDRetention + time.Second))
	if len(cache.entries) != 1 {
		t.Fatal("ambiguous entry was not refreshed by a later observation")
	}
	// A fresh observation can establish a mapping at the exact expiry boundary.
	cache.remember("id", "attestations", refreshed.Add(MessageIDRetention))
	if got := cache.topic("id", refreshed.Add(MessageIDRetention)); got != "attestations" {
		t.Fatalf("topic relearned after ambiguity expired = %q, want attestations", got)
	}
}

func TestTopicCacheRelearnExpiredMapping(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	cache.remember("id", "blocks", now)
	cache.remember("id", "attestations", now.Add(MessageIDRetention))
	if got := cache.topic("id", now.Add(MessageIDRetention)); got != "attestations" {
		t.Fatalf("topic after old mapping expired = %q, want attestations", got)
	}
}

func TestTopicCacheUnknownAndEmptyValues(t *testing.T) {
	cache := newTopicCache()
	now := time.Unix(1_700_000_000, 0)
	if got := cache.topic("missing", now); got != "" {
		t.Fatalf("unseen topic = %q, want unknown", got)
	}
	cache.remember("missing", "", now)
	if len(cache.entries) != 0 {
		t.Fatal("empty topic created an entry")
	}
	cache.remember("", "blocks", now)
	if got := cache.topic("", now); got != "blocks" {
		t.Fatalf("topic for empty message ID = %q, want blocks", got)
	}
	cache.remember("", "", now.Add(time.Minute))
	if got := cache.topic("", now.Add(time.Minute)); got != "blocks" {
		t.Fatalf("empty topic changed a known mapping to %q", got)
	}
	if got := cache.topic("", now.Add(MessageIDRetention)); got != "" {
		t.Fatalf("empty topic refreshed mapping to %q, want expired", got)
	}
}
