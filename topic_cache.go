package gossipsubtrace

import (
	"container/list"
	"time"
)

// MessageIDRetention is how long an observed message ID's topic is retained
// after the most recent IHAVE or published-message observation of that ID.
const MessageIDRetention = 120 * time.Second

// MessageIDSweepInterval controls how often expired message ID mappings are
// removed from the cache even when they are not looked up again.
const MessageIDSweepInterval = 10 * time.Second

type topicEntry struct {
	id        string
	topic     string
	expiresAt time.Time
}

// topicCache is protected by its owner's mutex. Observations must be supplied
// in chronological order, keeping expiry ordered from the oldest entry at the
// front to the newest entry at the back. Entries expire by age only; there is
// no size limit that could discard a recently observed mapping.
type topicCache struct {
	entries map[string]*list.Element
	expiry  list.List
}

func newTopicCache() topicCache {
	return topicCache{entries: make(map[string]*list.Element)}
}

func (c *topicCache) remember(id, topic string, now time.Time) {
	if topic == "" {
		return
	}
	if c.entries == nil {
		c.entries = make(map[string]*list.Element)
	}
	element, exists := c.entries[id]
	if !exists {
		c.entries[id] = c.expiry.PushBack(&topicEntry{
			id: id, topic: topic, expiresAt: now.Add(MessageIDRetention),
		})
		return
	}
	entry := element.Value.(*topicEntry)
	if !now.Before(entry.expiresAt) {
		entry.topic = topic
	} else if entry.topic != topic {
		// An ID advertised under multiple topics cannot be attributed safely.
		// Keep it ambiguous until all observations of it have expired.
		entry.topic = ""
	}
	entry.expiresAt = now.Add(MessageIDRetention)
	c.expiry.MoveToBack(element)
}

func (c *topicCache) topic(id string, now time.Time) string {
	element, exists := c.entries[id]
	if !exists {
		return ""
	}
	entry := element.Value.(*topicEntry)
	if !now.Before(entry.expiresAt) {
		delete(c.entries, id)
		c.expiry.Remove(element)
		return ""
	}
	return entry.topic
}

func (c *topicCache) cleanup(now time.Time) {
	for element := c.expiry.Front(); element != nil; element = c.expiry.Front() {
		entry := element.Value.(*topicEntry)
		if now.Before(entry.expiresAt) {
			return
		}
		delete(c.entries, entry.id)
		c.expiry.Remove(element)
	}
}
