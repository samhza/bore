package filedb

import (
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(
		func(tx *bolt.Tx) error {
			_, err = tx.CreateBucketIfNotExists([]byte("files"))
			if err != nil {
				return err
			}
			return nil
		})
	return &DB{db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

type DB struct {
	db *bolt.DB
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	tx, err := db.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

type Tx struct {
	tx *bolt.Tx
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Put(filename string, tags []string) error {
	has := make(map[string]struct{})
	var sortedtags []string
	for _, tag := range tags {
		if _, ok := has[tag]; !ok {
			sortedtags = append(sortedtags, tag)
			has[tag] = struct{}{}
		}
	}
	sort.Strings(sortedtags)
	return tx.fileB().Put([]byte(filename),
		[]byte(strings.Join(sortedtags, "\x00")))
}

func (tx *Tx) Get(filename string) (tags []string) {
	b := tx.tx.Bucket([]byte("files"))
	v := b.Get([]byte(filename))
	if v == nil {
		return nil
	}
	return strings.Split(string(v), "\x00")
}

func (tx *Tx) ForEach(fn func(Entry) error) error {
	return tx.fileB().ForEach(func(k, v []byte) error {
		return fn(Entry{string(k), strings.Split(string(v), "\x00")})
	})
}

func (tx *Tx) fileB() *bolt.Bucket {
	return tx.tx.Bucket([]byte("files"))
}

func Matches(ent Entry, incl, excl []string) bool {
	has := make(map[string]struct{}, len(ent.Tags))
	for _, tag := range ent.Tags {
		has[tag] = struct{}{}
	}
	for _, req := range incl {
		if _, ok := has[req]; !ok {
			return false
		}
	}
	for _, cant := range excl {
		if _, ok := has[cant]; ok {
			return false
		}
	}
	return true
}

func (tx *Tx) Rename(tag, newtag string) error {
	cur := tx.fileB().Cursor()
	cur.First()
	for k, v := cur.Next(); k != nil; {
		tags := strings.Split(string(v), "\x00")
		var oldI, newI int = -1, -1
		for i, t := range tags {
			if t == tag {
				oldI = i
			} else if t == newtag {
				newI = i
			}
		}
		if oldI == -1 {
			continue
		}
		if newI == -1 {
			tags[oldI] = newtag
		} else {
			tags[oldI] = tags[len(tags)-1]
			tags = tags[:len(tags)-1]
		}
		sort.Strings(tags)
		tx.fileB().Put(k, []byte(strings.Join(tags, "\x00")))
	}
	return nil
}

func (tx *Tx) Move(name, newname string) error {
	b := tx.fileB()
	v := b.Get([]byte(name))
	if err := b.Put([]byte(newname), v); err != nil {
		return err
	}
	return b.Delete([]byte(name))
}

func (tx *Tx) Len() int {
	return tx.fileB().Stats().KeyN
}

func (tx *Tx) Delete(name string) error {
	return tx.fileB().Delete([]byte(name))
}

func (tx *Tx) Cursor() *Cursor {
	return &Cursor{tx.fileB().Cursor()}
}

type Entry struct {
	Filename string
	Tags     []string
}

type Cursor struct {
	cur *bolt.Cursor
}

func (c *Cursor) First() *Entry {
	return kvToEntPtr(c.cur.First())
}
func (c *Cursor) Last() *Entry {
	return kvToEntPtr(c.cur.Last())
}
func (c *Cursor) Next() *Entry {
	return kvToEntPtr(c.cur.Next())
}
func (c *Cursor) Prev() *Entry {
	return kvToEntPtr(c.cur.Prev())
}
func (c *Cursor) Seek(name string) *Entry {
	return kvToEntPtr(c.cur.Seek([]byte(name)))
}
func (c *Cursor) Delete() error {
	return c.cur.Delete()
}

func kvToEntPtr(k, v []byte) *Entry {
	if k == nil {
		return nil
	}
	return &Entry{string(k), strings.Split(string(v), "\x00")}
}
