package main

import (
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v4"
)

func main() {
	// 打开DB
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)

	}

	defer db.Close()

	// 读写事物
	err = db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte("answer"), []byte("42"))
		txn.Get([]byte("answer"))
		return nil
	})
	// 只读事物
	err = db.View(func(txn *badger.Txn) error {
		txn.Get([]byte("answer"))
		return nil
	})

	// 遍历keys（范围查询）
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10      //指定预读取的尺寸大小
		it := txn.NewIterator(opts) //这个顶级迭代器屏蔽了数据可能在内存，也可能在外存，也可能在事务中，统一进行遍历
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() { // Rewind把指针指在哪里，Valid判断当前kv是否有效，Next取下一个kv
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	// vlog 的GC
	err = db.RunValueLogGC(0.7)
	_ = err

	// your code here
}
