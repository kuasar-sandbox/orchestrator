package execadmission

import (
	"container/list"
	"sync"
)

type cacheEntry struct {
	key string
	set *ProgramSet
}

type programCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*list.Element
	lru      *list.List
}

func newProgramCache(capacity int) *programCache {
	return &programCache{
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		lru:      list.New(),
	}
}

func (c *programCache) Get(key string) *ProgramSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil
	}
	c.lru.MoveToFront(element)
	return element.Value.(cacheEntry).set
}

func (c *programCache) Add(key string, set *ProgramSet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		element.Value = cacheEntry{key: key, set: set}
		c.lru.MoveToFront(element)
		return
	}
	element := c.lru.PushFront(cacheEntry{key: key, set: set})
	c.entries[key] = element
	for c.lru.Len() > c.capacity {
		oldest := c.lru.Back()
		delete(c.entries, oldest.Value.(cacheEntry).key)
		c.lru.Remove(oldest)
	}
}

func (c *programCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}
