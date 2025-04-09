package main

import (
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v4"
)

func main() {
	// NOTE:下面是通用的一些方法

	// 打开DB
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)

	}

	defer db.Close()
	// 读写事物
	// 在读写事务中允许所有数据库操作。
	err = db.Update(func(txn *badger.Txn) error {
		txn.Set([]byte("answer"), []byte("42"))
		txn.Get([]byte("answer"))

		// 或者下面这种set方式
		e := badger.NewEntry([]byte("answer"), []byte("42"))
		err := txn.SetEntry(e)
		return err
	})
	// 只读事务
	// 您不能在此事务中执行任何写入或删除。Badger 确保您在此闭包中获得一致的数据库视图。事务开始后在其他地方发生的任何写入, 都不会被闭包内的调用看到。
	err = db.View(func(txn *badger.Txn) error {
		txn.Get([]byte("answer"))
		return nil
	})

	// 遍历keys（范围查询），貌似这个代码块没有设置前缀，所以，会把所有数据都返回，然后在下面这个函数参数内进行全部遍历
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10 //指定预读取的kv对个数

		// opts.PrefetchValues = false // NOTE:注意上面那一行删掉，然后本行打开，就会变成仅键迭代模式，具体如下
		// Badger 支持一种独特的迭代模式, 称为key-only迭代。它比常规迭代快几个数量级, 因为它只涉及对 LSM 树的访问, 它通常完全驻留在 RAM 中。要启用仅键迭代, 您需要将该IteratorOptions.PrefetchValues 字段设置为false.
		// 这也可用于在迭代期间对选定键进行稀疏读取, item.Value()仅在需要时调用。

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

	//	NOTE:下面是另一种实现事务查询的方式

	// 另外，DB.View()和DB.Update()方法可以被DB.NewTransaction()和Txn.Commit()方法封装 (或者在只读事务下使用Txn.Discard())
	// 这些辅助方法将启动事务, 执行一个函数, 然后在返回错误时安全地丢弃您的事务。这是使用 Badger 交易的推荐方式。
	// DB.NewTransaction(), 接受一个布尔参数来指定是否需要读写事务。
	// 对于读写事务, 需要调用Txn.Commit() 以确保事务被提交。
	// 对于只读事务, 调用 Txn.Discard()就足够了。
	// Txn.Commit()也在内部调用Txn.Discard()以清理事务, 因此只需调用即可Txn.Commit()完成读写事务。
	// 但是, 如果由于某种原因没有调用 Txn.Commit()(例如, 它过早地返回错误), 那么请确保您Txn.Discard()在一个defer块中调用, 如:

	// Start a writable transaction.
	txn := db.NewTransaction(true)
	defer txn.Discard()
	if err1 := txn.Set([]byte("answer"), []byte("42")); err1 != nil {
		//抛出错误
	}
	if err2 := txn.Commit(); err2 != nil {
		//抛出错误
	}
}
