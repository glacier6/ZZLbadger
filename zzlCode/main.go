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
	// 只读事务
	err = db.View(func(txn *badger.Txn) error {
		txn.Get([]byte("answer"))
		return nil
	})

	// 遍历keys（范围查询），貌似这个代码块没有设置前缀，所以，会把所有数据都返回，然后在下面这个函数参数内进行全部遍历
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10      //指定预读取的kv对个数
		it := txn.NewIterator(opts) //NOTE:核心操作，这个顶级迭代器屏蔽了数据可能在内存，也可能在外存，也可能在事务中，统一进行遍历
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() { // Rewind把指针指向遍历的初始位置以及一些初始操作（核心函数），Valid判断当前kv是否有效，Next将指针指向下一个kv
			item := it.Item() // 取出当前遍历器指向的kv
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v) //输出取出来的kv对
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	// vlog 的GC
	err = db.RunValueLogGC(0.7) //脏键百分比0.7
	_ = err

	// your code here
	// if t.baseLevel != 6 {
	// 	fmt.Print("非第6层了")
	// }
}
