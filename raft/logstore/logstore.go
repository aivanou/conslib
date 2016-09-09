package logstore

import (
	"sync"
	"errors"
	"log"
)

type LogItem struct {
	Index uint32
	Data  uint64
	Term  uint64
}

type LogStoreNode struct {
	LogItem
	next *LogStoreNode
	prev *LogStoreNode
}

type LogStore struct {
	head *LogStoreNode
	tail *LogStoreNode
	lock *sync.Mutex
	size uint32
}

func NewLogStore() *LogStore {
	store := new(LogStore)
	store.lock = &sync.Mutex{}
	return store
}

func (store *LogStore) IsEmpty() bool {
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.size == 0
}

func (store *LogStore) LastLogItem() *LogItem {
	if store.head == nil {
		return nil
	}
	var li *LogStoreNode = nil
	if (store.tail == nil) {
		li = store.head
	}else {
		li = store.tail
	}
	return &LogItem{li.Index, li.Data, li.Term}
}

func (store *LogStore) GetAllItemsAfter(index uint32) []LogItem {
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.gatherAfter(index)
}

func (store *LogStore) AppendLogItem(li *LogItem) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	item := &LogStoreNode{LogItem{li.Index, li.Data, li.Term}, nil, nil}
	if store.head == nil {
		item.Index = 1
		store.head = item
	}else if (store.tail == nil) {
		item.Index = 2
		store.tail = item
		store.tail.prev = store.head
		store.head.next = store.tail
	}else {
		item.Index = store.tail.Index + 1
		store.tail.next = item;
		item.prev = store.tail
		store.tail = item
	}
	store.incSize()
	return nil
}

func (store *LogStore) Append(data uint64, term uint64) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	item := &LogStoreNode{LogItem{0, data, term}, nil, nil}
	if store.head == nil {
		item.Index = 1
		store.head = item
	}else if (store.tail == nil) {
		item.Index = 2
		store.tail = item
		store.tail.prev = store.head
		store.head.next = store.tail
	}else {
		item.Index = store.tail.Index + 1
		store.tail.next = item;
		item.prev = store.tail
		store.tail = item
	}
	store.incSize()
	return nil
}

func (store *LogStore) RemoveByIndex(index uint32) *LogStoreNode {
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
		if store.head != nil {
			store.head.prev = nil
		}
	}else if item == store.tail {
		prev := store.tail.prev
		if (prev == store.head) {
			store.tail.prev = nil
			store.head.next = nil
			store.tail = nil
		}else {
			store.tail = prev
			prev.prev = nil
			store.tail.next = nil
		}
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
		store.head.next = nil
		store.head = nil
		if (store.tail != nil) {
			store.tail.prev = nil
		}
		store.tail = nil
		store.updateSize(0)
	}else if item == store.tail {
		store.tail = item.prev
		store.tail.next = nil
		if (store.tail == store.head) {
			store.tail = nil
		}
		store.decSize()
	}else {
		store.updateSize(store.size - countNodes(item))
		prev := item.prev
		store.tail = prev
		store.tail.next = nil
	}
	return true
}

func (store *LogStore) FindByIndex(index uint32) *LogStoreNode {
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.findByIndex(index)
}

func countNodes(node *LogStoreNode) uint32 {
	count := uint32(0)
	for node != nil {
		count += 1
		node = node.next
	}
	return count
}

func (store *LogStore) findByIndex(index uint32) *LogStoreNode {
	if store.head == nil {
		return nil
	}
	item := store.head
	for item != nil {
		if item.Index == index {
			return item
		}
		item = item.next
	}
	return nil
}

func (store *LogStore) findOrNextGreater(index uint32) *LogStoreNode {
	if store.head == nil {
		return nil
	}
	item := store.head
	for item != nil {
		if item.Index >= index {
			return item
		}
		item = item.next
	}
	return nil
}

func (store *LogStore) gatherAfter(index uint32) []LogItem {
	items := make([]LogItem, 0, 0)
	item := store.findOrNextGreater(index)
	if item == nil {
		return items
	}
	for item != nil {
		items = append(items, LogItem{item.Index, item.Data, item.Term})
		item = item.next
	}
	return items
}

func (store *LogStore) Size() uint32 {
	store.lock.Lock()
	defer store.lock.Unlock()
	size := store.size
	return size
}

func (store *LogStore) Print() {
	store.lock.Lock()
	defer store.lock.Unlock()
	store.print()
}

func (store *LogStore) print() {
	curr := store.head
	for curr != nil {
		log.Printf("[%d:%d]->", curr.Index, curr.Term)
		curr = curr.next
	}
	log.Println()
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