package levelbtree

import (
	"encoding/json"
	"github.com/jmhodges/levigo"
	"log"
)

type Tree struct {
	iter      levigo.Iterator
	rootKey   []byte
	keyPrefix []byte
	nowBranch Branch
}

func NewTree(db *levigo.db) (*Tree, err) {
	ro := levigo.NewReadOptions()
	t := &Tree{
		iter:      db.NewIterator(ro),
		rootKey:   []byte{0, 0},
		keyPrefix: []byte{0},
	}
	t.switchRootBranch()
	return t, nil
}

func (t *Tree) Set(key []byte, value []byte) error {
	t.switchRootBranch()

}

func (t *Tree) switchRootBranch() err {
	t.iter.Seek(rootKey)
	branchRaw := t.iter.Value()
	if t.iter.Valid() == false {

	}
	var branch Branch
	json.Unmarshal(branchRaw, &branch)
	t.nowBranch = brach
}

func (t *Tree) Get(key []byte) ([]byte, error) {

}

func (t *Tree) Offset(offset int) (levigo.Iterator, error) {

}

type Branch struct {
	valueNum  int
	key       []byte
	parentKey []byte
	childKeys [][]byte
	maxChild  int
}

func NewBranch(t *Tree, parentKey, rootKey ...[]byte) *Branch {
	var key []byte
	if len(parentKey) == 0 {
		key = rootKey[0]
	} else {
		key = t.searchClassKey
	}

	branch := &Branch{
		valueNum:  0,
		key:       key,
		parentKey: parentKey,
		maxChild:  10,
	}
}
