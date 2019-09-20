package skiplist

import "sort"

const (
	MaxLevel = 10
)

type SkipList struct {
	level  int32
	less   Less
	levels [][]*Element
}

func New(level int32, less Less) *SkipList {
	return &SkipList {
		level: level,
		less:  less,
	}
}

func (l *SkipList) Len() int {

}

func (l *SkipList) Remove(element *Element) {

}

func (l *SkipList) RemoveGreater(element *Element, equal bool) int {

}

func (l *SkipList) RemoveLess(element *Element, equal bool) int {

}

func (l *SkipList) Largest() *Element {

}

func (l *SkipList) Smallest() *Element {

}

func (l *SkipList) FindGreater() *Element{

}

func (l *SkipList) FindLess() *Element {

}

func (l *SkipList) Insert(v interface{}) *Element {

}

const (
	euqualRes = 0
	greaterRes = 0
	lessRes = 0
)

func (l *SkipList) find(v interface{}) (elem *Element, res int) {
	var start, end *Element
	var level = 0

	for level = len(l.levels) - 1; level >= 0; level-- {
		elems := l.levels[level]
		length := len(elems)
		if len(elems) == 0 {
			res = greaterRes
			return
		}

		i := sort.Search(length, func(i int) bool {
			return l.less(elems[i].Value, v)
		})

		if i == 0 {
			res = lessRes
			break
		}

		if i == length {
			res = greaterRes
			break
		}

		res = euqualRes
		start = elems[i]
		end = start.nextLevel(level)
	}
}

type Element struct {
	pre []*Element
	next []*Element
	Value interface{}
}

func (elem *Element) nextLevel(level int) *Element {
	if len(elem.next) <= level {
		return nil
	}
	return elem.next[level]
}

func (elem *Element) preLevel(level int) *Element {
	if len(elem.pre) <= level {
		return nil
	}
	return elem.pre[level]
}


type Less func(x, y interface{}) bool