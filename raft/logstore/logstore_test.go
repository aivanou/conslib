package logstore

// import (
// 	"testing"
// 	"math/rand"
// 	"github.com/stretchr/testify/assert"
// )
//
// func TestLogStoreEmpty(t *testing.T) {
// 	store := NewLogStore()
// 	resObj := store.RemoveByIndex(1)
// 	assert.Nil(t, resObj, "Remove from empty store should return nil")
// 	res := store.RemoveAfterExc(1)
// 	assert.False(t, res, "Remove from empty store should return false")
// 	res = store.RemoveAfterIncl(1)
// 	assert.False(t, res, "Remove from empty store should return false")
// 	resObj = store.FindByIndex(1)
// 	assert.Nil(t, resObj, "FindByIndex on empty logstore should return nil")
// 	size := store.Size()
// 	assert.Equal(t, uint32(0), size)
// }
//
// func TestLogStoreAppendEntries(t *testing.T) {
// 	store := NewLogStore()
// 	store.Append(1, 1)
// 	assert.NotNil(t, store.head)
// 	size := store.Size()
// 	assert.Equal(t, uint32(1), size)
// 	res := store.FindByIndex(1)
// 	assert.NotNil(t, res)
// 	rand.Seed(123112)
// 	count := int(rand.Int31n(200))
// 	for i := 0; i < count; i++ {
// 		store.Append(uint64(rand.Uint32()), uint64(i + 1))
// 	}
// 	size = store.Size()
// 	assert.Equal(t, uint32(count + 1), size)
// }
//
// func TestLogStoreRemove(t *testing.T) {
// 	store := NewLogStore()
// 	rand.Seed(123)
// 	count := 300
// 	for i := 0; i < count; i++ {
// 		store.Append(uint64(rand.Uint32()), uint64(i + 1))
// 	}
//
// 	assert.Equal(t, uint32(300), store.Size())
// 	store.RemoveByIndex(250)
// 	store.RemoveByIndex(300)
// 	store.RemoveByIndex(1)
// 	assert.Equal(t, uint32(297), store.Size())
// 	assert.Nil(t, store.FindByIndex(250))
// 	assert.Nil(t, store.FindByIndex(300))
// 	assert.Nil(t, store.FindByIndex(1))
// 	store.RemoveAfterExc(100)
// 	assert.Equal(t, uint32(99), store.Size())
// 	store.RemoveAfterIncl(40)
// 	assert.Equal(t, uint32(38), store.Size())
//
// }
//
// func TestAppendLogItemObjects(t *testing.T) {
// 	store := NewLogStore()
// 	items := GenRandomLogItems(10, uint64(1), uint32(1))
// 	for _, it := range items {
// 		store.AppendLogItem(&it)
// 	}
// 	assert.Equal(t, uint32(10), store.Size())
// 	assert.Equal(t, uint32(10), store.LastLogItem().Index)
// }
//
// func TestGetLogsAfter(t *testing.T) {
// 	store := NewLogStore()
// 	items := GenRandomLogItems(10, uint64(1), uint32(1))
// 	for _, it := range items {
// 		store.AppendLogItem(&it)
// 	}
// 	assert.Equal(t, 10, len(store.GetAllItemsAfter(0)))
// }
//
// func GenRandomLogItems(size int, term uint64, startIndex uint32) []LogItem {
// 	items := make([]LogItem, size)
// 	for i := 0; i < size; i++ {
// 		items[i] = LogItem{startIndex + uint32(i), uint64(i), term}
// 	}
// 	return items
// }
