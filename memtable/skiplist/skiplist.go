package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cerdore/cdb/memtable/interfaces"

	log "github.com/sirupsen/logrus"
)

// TODO: make configurable?
const maxLevels = 12
const kBranching = 4

// Node represents a node in the SkipList structure
type Node struct {
	next    []*Node
	key     []byte
	value   []byte
	deleted bool
}

// SkipList is an implementation of a data structure that provides
// O(log n) insertion and removal without complicated self-balancing logic
// required of similar tree-like structures (e.g. red/black, AVL trees)
// See the following for more details:
//   - https://en.wikipedia.org/wiki/Skip_list
//   - https://igoro.com/archive/skip-lists-are-fascinating/
type SkipList struct {
	head   *Node
	levels int
	size   uint32
	num    uint32
}

var _ interfaces.InMemoryStore = &SkipList{}

func New(seed int64) *SkipList {
	rand.Seed(seed)

	return &SkipList{
		head:   &Node{next: make([]*Node, maxLevels)},
		levels: 1,
	}
}

// Get returns a boolean indicating whether the specified key
// was found in the list. If true, the value is returned as well
func (s *SkipList) Get(key []byte) (bool, []byte) {
	return s.get(key)
}

func (s *SkipList) get(key []byte) (bool, []byte) {
	c := s.head
	for i := s.levels - 1; i >= 0; i-- {
	rightTraversal:
		for ; c.next[i] != nil; c = c.next[i] {
			switch bytes.Compare(c.next[i].key, key) {
			case 0:
				if c.next[i].deleted {
					return true, nil
				} else {
					return true, c.next[i].value
				}
			case 1: // next key is greater than the key we're searching for
				break rightTraversal
			}
		}
	}

	return false, nil
}

// Put inserts or updates the value if the key already exists
func (s *SkipList) Put(key []byte, value []byte) {
	shouldUpdate, oldValue := s.get(key)
	shouldUpdate = shouldUpdate || s.isDeleted(key)

	if shouldUpdate {
		s.update(key, value)
		s.size += uint32(len(value) - len(oldValue))
	} else {
		s.insert(key, value)
		s.size += uint32(len(key) + len(value))
		s.num++
	}

}

// Removes the specified key from the skip list. Returns true if
// key was removed and false if key was not present
func (s *SkipList) Delete(key []byte) bool {
	// TODO: needs to account for deletes of keys that may not currently be in memtable (this allows us to delete
	// keys that may exist in sstable)
	c := s.head
	removed := false
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) && !c.next[i].deleted {
				c.next[i].deleted = true
				removed = true
				break
			}
		}
	}
	if !removed {
		s.insertD(key, nil)
		s.size += uint32(len(key))
		s.num++
	}
	return removed
}

func (s *SkipList) update(key []byte, value []byte) {
	c := s.head
	updated := false
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) {
				c.next[i].value = value
				c.next[i].deleted = false
				updated = true
				break
			}
		}
	}

	if !updated {
		log.Panicf("could not update key %v (%s) even though we expected it to exist!", key, string(key))
	}
}

func (s *SkipList) insert(key []byte, value []byte) {
	levels := s.generateLevels()

	if levels > s.levels {
		s.levels = levels
	}

	newNode := &Node{next: make([]*Node, levels), key: key, value: value, deleted: false}

	c := s.head
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			// Stop moving rightward at this level if next key is greater
			// than key we plan to insert
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) {
				log.Panicf("attempting to insert key %v (%s) that already exists. "+
					"this should not happen!", key, string(key))
			}
		}
		if levels > i {
			newNode.next[i] = c.next[i]
			c.next[i] = newNode
		}
	}
}

func (s *SkipList) insertD(key []byte, value []byte) {
	levels := s.generateLevels()

	if levels > s.levels {
		s.levels = levels
	}

	newNode := &Node{next: make([]*Node, levels), key: key, value: value, deleted: true}

	c := s.head
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			// Stop moving rightward at this level if next key is greater
			// than key we plan to insert
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) {
				log.Panicf("attempting to insert key %v (%s) that already exists. "+
					"this should not happen!", key, string(key))
			}
		}
		if levels > i {
			newNode.next[i] = c.next[i]
			c.next[i] = newNode
		}
	}
}
func (s *SkipList) isDeleted(key []byte) bool {
	c := s.head
	for i := s.levels - 1; i >= 0; i-- {
		for ; c.next[i] != nil; c = c.next[i] {
			if bytes.Compare(c.next[i].key, key) > 0 {
				break
			} else if bytes.Equal(c.next[i].key, key) {
				return c.next[i].deleted
			}
		}
	}

	return false
}

// Print prints skip list in a pretty format. Should only be used for debugging
// Not particularly efficient. Would not recommend on larger lists
// TODO: represent deleted keys
func (s *SkipList) Print() {
	keysLoc := map[string]int{}
	idx := 1
	for node := s.head.next[0]; node != nil; node = node.next[0] {
		keysLoc[string(node.key)] = idx
		idx++
	}
	nodeWidth := 10

	for i := s.levels - 1; i >= 0; i-- {
		s.printNodeBorder(i, keysLoc, nodeWidth)
		fmt.Println()
		s.printNode(i, keysLoc, nodeWidth)
		fmt.Println()
		s.printNodeBorder(i, keysLoc, nodeWidth)
		fmt.Println()
	}
}

func (s *SkipList) printNodeBorder(i int, keysLoc map[string]int, nodeWidth int) {
	nextSlot := 1
	for node := s.head.next[i]; node != nil; node = node.next[i] {
		loc := keysLoc[string(node.key)]

		for nextSlot != loc {
			fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat(" ", nodeWidth))
			fmt.Print(" ")
			nextSlot++
		}

		fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat("-", nodeWidth))
		fmt.Print(" ")
		nextSlot++
	}
}

func (s *SkipList) printNode(i int, keysLoc map[string]int, nodeWidth int) {
	nextSlot := 1
	for node := s.head.next[i]; node != nil; node = node.next[i] {
		loc := keysLoc[string(node.key)]

		keySize := 4
		key := string(node.key)
		if len(key) > keySize {
			key = key[0:keySize]
		} else if len(key) < keySize {
			key = fmt.Sprintf(fmt.Sprint("%-", keySize, "s"), key)
		}

		for nextSlot != loc {
			fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), strings.Repeat(" ", nodeWidth))
			fmt.Print(" ")
			nextSlot++
		}

		fmt.Printf(fmt.Sprint("%", nodeWidth, "s"), "|   "+key+" |")
		fmt.Print(" ")
		nextSlot++
	}
}

// Level generation shamelessly stolen from
//https://igoro.com/archive/skip-lists-are-fascinating/
func (s *SkipList) generateLevels() int {
	// levels := 0
	// for num := rand.Int31(); num&1 == 1; num >>= 1 {
	// 	levels += 1
	// }

	// if levels == 0 {
	// 	levels = 1
	// }

	// return levels

	height := 1
	for height < maxLevels && (rand.Intn(kBranching) == 0) {
		height++
	}
	return height
}

func (s *SkipList) InternalIterator() interfaces.InternalIterator {
	return NewIterator(s)
}

func (s *SkipList) Size() uint32 {
	return s.size
}

func (s *SkipList) Num() uint32 {
	return s.num
}
