package mem

import (
	"sync"
	"sync/atomic"
)

var pools []*sync.Pool

const (
	minSize = 1024
)

func bitCount(size int) (count int) {
	for ; size > minSize; count++ {
		size = (size + 1) >> 1
	}
	return
}

// Инициализируем массив пулов разных размеров.
// Переопределяем функцию New. Размеры буферов выравниваются по 1024 байта
func init() {
	// 1KB ~ 256MB
	pools = make([]*sync.Pool, bitCount(1024*1024*256))
	for i := 0; i < len(pools); i++ {
		slotSize := 1024 << i
		pools[i] = &sync.Pool{
			New: func() interface{} {
				buffer := make([]byte, slotSize)
				return &buffer
			},
		}
	}
}

// Выбираем пулл по размеру из массива пулов
func getSlotPool(size int) (*sync.Pool, bool) {
	index := bitCount(size)
	if index >= len(pools) {
		return nil, false
	}
	return pools[index], true
}

// В дальнейшем можно будет использовать для метрик
var total int64

// Allocate - Получаем буфер по размеру из массива пулов.
// Размер буфера выравнивается по 1024 байтам.
// Буфер переиспользуется из ранее аллоцированных, если в пуле
// нет свободного буфера, то выделяется память и создаётся новый буфер.
func Allocate(size int) []byte {
	if pool, found := getSlotPool(size); found {
		atomic.AddInt64(&total, 1)
		//glog.V(4).Infof("++> %d", newVal)
		//glog.Infof("++> %d", newVal)

		slab := *pool.Get().(*[]byte)
		return slab[:size]
	}
	return make([]byte, size)
}

// Free - Освободить буфер и венуть его в пул.
func Free(buf []byte) {
	if pool, found := getSlotPool(cap(buf)); found {
		atomic.AddInt64(&total, -1)
		//glog.V(4).Infof("--> %d", newVal)
		//glog.Infof("--> %d", newVal)

		pool.Put(&buf)
	}
}
