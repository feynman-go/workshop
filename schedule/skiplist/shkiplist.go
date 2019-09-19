package skiplist

import "sync"

type SkipList struct {
	level int
}


type Compare interface {
	Less(i, j interface{}) bool
}

type node struct {
	rw sync.RWMutex
	item Item

	level

	[]*node

}

type Item struct {
	Next Item
}