package main

import (
	"testing"
	"math/rand"
	"fmt"
)

func TestLogStoreEmpty(t *testing.T) {
	store := NewLogStore()
	resObj := store.RemoveByIndex(1)
	if resObj != nil {
		t.Error("Remove from empty store should return nil")
	}
	res := store.RemoveAfterExc(1)
	if res {
		t.Error("Remove from empty store should return false")
	}
	res = store.RemoveAfterIncl(1)
	if res {
		t.Error("Remove from empty store should return false")
	}
	resObj = store.FindByIndex(1)
	if resObj != nil {
		t.Error("FindByIndex on empty logstore should return nil")
	}
	size := store.Size()
	if size != 0 {
		t.Error("Size should return 0 if empty store")
	}
}

func TestLogStoreAppendEntries(t *testing.T) {
	store := NewLogStore()
	store.Append(1, 1)
	if store.head == nil {
		t.Error("Log item wasn't added")
	}
	size := store.Size()
	if size != 1 {
		t.Error("Size is not 1")
	}
	res := store.FindByIndex(1)
	if res == nil {
		t.Error("Log item was added but cannot find it")
	}
	rand.Seed(123112)
	count := int(rand.Int31n(200))
	for i := 0; i < count; i++ {
		store.Append(uint64(rand.Uint32()), uint64(i + 1))
	}
	size = store.Size()
	if size != uint32(count + 1) {
		t.Error("Expected: ", (count + 1), " got: ", size)
	}
}

func TestLogStoreRemove(t *testing.T) {
	store := NewLogStore()
	rand.Seed(123)
	count := 300
	for i := 0; i < count; i++ {
		store.Append(uint64(rand.Uint32()), uint64(i + 1))
	}
	checkSize(store, 300, t)
	store.RemoveByIndex(250)
	store.RemoveByIndex(300)
	store.RemoveByIndex(1)
	checkSize(store, 297, t)
	fmt.Println("")
	errorIfNotNil(store.FindByIndex(250), t)
	errorIfNotNil(store.FindByIndex(300), t)
	errorIfNotNil(store.FindByIndex(1), t)
	store.RemoveAfterExc(100)
	checkSize(store, 99, t)
	store.RemoveAfterIncl(40)
	checkSize(store, 38, t)
}

func errorIfNotNil(obj *LogItem, t *testing.T) {
	if obj != nil {
		t.Error("LogItem is not null", obj)
	}
}

func checkSize(store *LogStore, expectedSize uint32, t *testing.T) {
	size := store.Size()
	if size != expectedSize {
		t.Error("Expected: ", expectedSize, " got: ", size)
	}
}


