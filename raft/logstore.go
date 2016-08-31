package main
import (
	"sync"
	"errors"
)

type LogItem struct {
	index uint32
	data  uint64
	term  uint64
	next  *LogItem
	prev  *LogItem
}

type LogStore struct {
	head *LogItem
	tail *LogItem
	lock *sync.Mutex
	size uint32
}

func NewLogStore() *LogStore {
	store := new(LogStore)
	store.lock = &sync.Mutex{}
	return store
}

func (store *LogStore) Append(data uint64, term uint64) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	item := &LogItem{0, data, term, nil, nil}
	if store.head == nil {
		item.index = 1
		store.head = item
	}else if (store.tail == nil) {
		item.index = 2
		store.tail = item
		store.tail.prev = store.head
		store.head.next = store.tail
	}else {
		item.index = store.tail.index + 1
		store.tail.next = item;
		item.prev = store.tail
		store.tail = item
	}
	store.incSize()
	return nil
}

func (store *LogStore) RemoveByIndex(index uint32) *LogItem {
	store.lock.Lock()
	defer store.lock.Unlock()
	if store.head == nil {
		return nil
	}
	item := store.findByIndex(index)
	if item == nil {
		return nil
	}else if item == store.head {
		store.head = store.head.next
		store.head.prev = nil
	}else if item == store.tail {
		store.tail = store.tail.prev
		store.tail.next = nil
	}else {
		prev := item.prev
		next := item.next
		prev.next = next
		next.prev = prev
		item.prev = nil
		item.next = nil
	}
	store.decSize()
	return item
}

func (store *LogStore) RemoveAfterExc(index uint32) bool {
	store.lock.Lock()
	defer store.lock.Unlock()
	if store.head == nil {
		return false
	}
	item := store.findByIndex(index)
	if item == nil {
		return false
	}
	if item == store.head {
		store.head.next = nil
		store.head.prev = nil
		store.tail = nil
		store.updateSize(1)
	}else if item == store.tail {
		//do nothing
	}else {
		store.updateSize(store.size - countNodes(item.next))
		store.tail = item
		store.tail.next = nil
	}
	return true
}

func (store *LogStore) RemoveAfterIncl(index uint32) bool {
	store.lock.Lock()
	defer store.lock.Unlock()
	if store.head == nil {
		return false
	}
	item := store.findByIndex(index)
	if item == nil {
		return false
	}
	if item == store.head {
		store.head = nil
		store.tail = nil
		store.updateSize(0)
	}else if item == store.tail {
		store.tail = item.prev
		store.tail.next = nil
		store.updateSize(1)
	}else {
		store.updateSize(store.size - countNodes(item))
		prev := item.prev
		store.tail = prev
		store.tail.next = nil
	}
	return true
}

func (store *LogStore) FindByIndex(index uint32) *LogItem {
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.findByIndex(index)
}

func countNodes(node *LogItem) uint32 {
	count := uint32(0)
	for node != nil {
		count += 1
		node = node.next
	}
	return count
}

func (store *LogStore) findByIndex(index uint32) *LogItem {
	if store.head == nil {
		return nil
	}
	item := store.head
	for item != nil {
		if item.index == index {
			return item
		}
		item = item.next
	}
	return nil
}

func (store *LogStore) Size() uint32 {
	store.lock.Lock()
	defer store.lock.Unlock()
	size := store.size
	return size
}

func (store *LogStore) incSize() {
	store.size += 1
}

func (store *LogStore) decSize() error {
	if store.size == 0 {
		return errors.New("Cannot decrease size of 0")
	}
	store.size -= 1
	return nil
}

func (store* LogStore) updateSize(newSize uint32) {
	store.size = newSize
}