ZZL写在最前：
(1)目前badger的大致优化方向：
  1.热点识别与更新（利用有限的缓存进行更好的冷热分离）
  2.动态参数的调整（BadgerDB的参数目前是锁死的）
  3.zzlTODO:待看BadgerDB缓存是缓存整个KV对还是K OFFSET对，如果是前者的话可以考虑优化成K OFFSET对，即使其变成一个类似跳表的东西（用NVM实现）。然后这个东西还可以在GC的时候用（因为只存K OFFSET的话能存特别多KV对），又或者只记录日志合并时失效的KV对的KEY信息（不太现实），只用于GC

(2)BadgerDB的一些需要关注点
  1.SST分为元数据块，索引块，DATA块
  2.目录中文件名后面的带test的，是做单元测试的模块
  3.有两级缓存，一级索引的缓存，一个块的缓存
  4.数据可能在内存，也可能在外存，也可能在事务中，共三类。而对这三类查找时，每类均会有一个迭代器，每个迭代器的实现方式也不一样
  5.badger 中与事务相关的结构体包括 Txn 和 oracle 两个
    单个事务层面，在 Txn 结构体中，需要记录自己读写的 key 列表，以及事务的开始时间戳（readTs，用于读取）和提交时间戳（commitTs，用于在commit的时候加在写入的key后，注意这两个时间戳全是由o.nextTxnTs产生），以及读写的 key 列表。
    全局事务层面，在 oracle 结构体中，其相当于相当于事务管理器，需要管理全局时间戳，以及近期提交的事务列表（committedTxns），用于在新的事务提交中对事务开始与提交时间戳中间提交过的事务范围进行冲突检查，以及当前活跃的事务的最小时间戳，用于清理旧事务信息（指清理committedTxns数组，这个数组增加时是在newCommitTs的时候增加）。
    而WaterMark（水位） 结构体内部是个堆，用于管理、查找事务开始、结束的区段。oracle 的 txnMark 主要用于协调等待 Commit 授时与落盘的时间窗口，readMark 管理当前活跃事务的最早时间戳，用于清理过期的 committedTxns（在cleanupCommittedTransactions函数中，触发清理的有两处，可以搜o.cleanupCommittedTransactions()来查看）。
    5.1 事务时间戳是逻辑时间戳，每次事务提交时递增 1。
    5.2 只读事务是不进行提交操作的，所以没有commitTs。写入数据是在获取commitTs之后，读取应该是在获取commitTs之前.
    5.3 TODO: 那个事务过程笔记中，如果事务4是写入A事务，3是进行读取A的事务，会怎样（即写后读会咋样）
    5.4 SSI 事务中冲突探测的逻辑就是，找出在当前事务执行期间 Commit 的事务列表，检查当前事务读取的 key 列表是否与这些事务的写入的 key 列表有重叠。

  6.levelHandler中存的SST表，即tables存储的是那个SST的所有数据吗，还是只有一些关键信息？
    不是，里面有各个块的信息（这个信息貌似是以Mmap方式存储在内存的），找目标key先找到目标块，然后再看这个块是否在cache中，不在的话去磁盘找
  7.TODO: 索引缓存在哪里用的？
  8.TODO: mmap具体如何，何时返回磁盘的？（看ristretto怎么用即可）
  9.TODO: GC按理来说应该是定期会自动检查的（可能在日志合并的时候？），难道是需要自己去手动调RunValueLogGC这个函数吗？
  10.目前badger主要会有4类压缩，L0->L0,LMAX->LMAX,L0->Lbase(基线层)还有普通的Li-1到Li

(3)一些简称
  lc levercontroler 层级管理器
  lf logFile log文件对象
  vp valuePointer 值指针

(4)注意Badger 本身不进行分发，Dgraph 在其之上实现了一层来提供分布式功能。

(5)mmap是什么？及其特性，关键点 （BadgerDB内也使用了mmap，如Table,discardStats,logFile等都是由mmap实现的，可以搜索MmapFile来定位）
  详见 https://baijiahao.baidu.com/s?id=1676499929976516330&wfr=spider&for=pc
  首先，传统的IO模型进行磁盘数据读写时，一般大致需要2个步骤，拿写入数据为例：1.从用户空间拷贝到内核空间；2.从内核空间写入磁盘。
  1.mmap（memory mapped file）是一种内存映射文件的方法，即将一个文件或者其它对象映射到进程的地址空间，实现文件磁盘地址和进程虚拟地址空间中一段虚拟地址的一一对映关系.
  对文件进行Mmap后，会在进程的虚拟内存分配地址空间，创建与磁盘的映射关系。 实现这样的映射后，就可以以指针的方式读写操作映射的虚拟内存，系统则会自动回写磁盘；相反，内核空间对这段区域的修改也直接反映到用户空间，从而可以实现不同进程间的数据共享。与传统IO模式相比，减少了一次用户态copy到内核态的操作！！！也因此认为其比较快。
  2.mmap 由操作系统负责管理，对同一个文件地址的映射将被所有线程共享，操作系统确保线程安全以及线程可见性；
  3.需要注意的是，进行Mmap映射时，并不是直接申请与磁盘文件一样大小的内存空间；而是使用进程的地址空间与磁盘文件地址进行映射，当真正的文件读取是当进程发起读或写操作时。
  当进行IO操作时，发现用户空间内不存在对应数据页时（缺页），会先到交换缓存空间（swap cache）去读取，如果没有找到再去磁盘加载（调页）。
  4.其可以用在哪里
    进程间通信：从自身属性来看，Mmap具有提供进程间共享内存及相互通信的能力，各进程可以将自身用户空间映射到同一个文件的同一片区域，通过修改和感知映射区域，达到进程间通信和进程间共享的目的。
    大数据高效存取：对于需要管理或传输大量数据的场景，内存空间往往是够用的，这时可以考虑使用Mmap进行高效的磁盘IO，弥补内存的不足。例如RocketMQ，MangoDB等主流中间件中都用到了Mmap技术；总之，但凡需要用磁盘空间替代内存空间的时候都可以考虑使用Mmap。


(6)MVCC是什么？（在BadgerDB中用MVCC方式做事务处理）
  1.MVCC 英文全称叫 "Multi Version Concurrency Control"，翻译过来就是 "多版本并发控制"。在 MySQL 众多存储引擎中只有 InnoDB 存储引擎中实现了 MVCC 机制。MVCC是快照读，非普通的当前读
  2.简单来说的话，就是每个数据（BadgerDB中的每个KV对）都有多个版本（与LSM树特性一致），然后这些版本会构成一个版本链（undo log），这条链中，每个版本中会存一个trx_id(导致当前版本产生的事务id) 和roll_pointer(回滚指针，指向上一个版本)。然后决定读取操作数据哪个版本的，就取决于readView，其是事务在使用 MVCC 机制对数据库中的数据操作时产生的读视图，即当事务开启之后，会生成数据库当前系统的一个快照（这个快照最核心的是构造一个数据结构，包含一个数组（当前系统活跃事务的id，活跃指的是正在操作，但是还未提交的事务）与该数组中的最小ID值与预分配的事务ID（下一个事务ID）与创建该快照的事务ID。值得注意的是，每个快照会对应这样一个数据结构，而这个数据结构也正是处理事务处理时序问题的关键（貌似BadgerDB会在每个数据后拼接上时间戳来当版本号，然后每个数据的快照版本，就可以用这个时间戳来实现，具体的还需要更详细看代码！！！！！）
  而readView 的核心原理主要体现在 READ COMMITTD(读已提交)和 REPEATABLE READ(可重复读) 两种隔离级别上。（共有四种隔离级别，读未提交：解决了脏写问题；
                读已提交：解决了脏写，脏读问题；
                可重复读：解决了脏写，脏读，不可重复读；
                串行化：解决了脏写，脏读，不可重复读，幻读所有问题；）
  READ COMMITTD：在每次进行 SELECT 查询操作的时候都会去生成一个 readView；
  REPEATABLE READ：每开启一个事务才会生成一个 readView，一个事务的所有SQL语句共享一个 readView。（一般都是这个级别实现的，在这种情况下，相当于每一个事务在创建的时候都会存一个上面提到的那个快照的活跃事务ID数组，并以此来判断事务可以用到哪些数据，比如如果数据上附加的版本号小于快照中的min_trx_id，那么就可以用）
  一定一定要区分好读已提交和可重复读隔离级别下 readView 的生成时机，这是这两种隔离级别原理最最最核心的因素。再次强调！！！
  PS：在 InnoDB 存储引擎中，幻读的问题也得到了解决，解决的方式是利用间隙锁；
  PS：幻读与不可重复读都属于并发引发的问题，都指的是一个事务两次查询结果不一样，但是幻读通常针对整张表（指如整张表的统计数据），而不可重复读一般针对的是某一条数据或者某几条数据（即确切的某条数据）。


(7)badger具体实现了哪种隔离
  1.badger实现了可串行的快照隔离 (Serializable Snapshot Isolation, SSI)
  2.SSI是一种乐观的并发控制机制。它的原则是事情可能导致出错，但是不管它，所有事务都继续执行，SSI希望最后能得到正确的结果。所以让大家都直接执行，直到事务结束之后，SSI去检查是否结果有并发问题。如果有，那就回滚这个事务。
  3.所以，SSI (可串行的快照隔离) 比起之前的快照隔离，区别在于：SSI多了检测串行化冲突和决定哪些事务需要回滚的机制。
  详见 https://zhuanlan.zhihu.com/p/395229054

(8)GO语言
  1.<- chan 如果取不出来数据，会把当前执行环境阻塞的
  2.for range用于 for 循环中迭代数组(array)、切片(slice)、通道(channel)或集合(map)的元素。在数组和切片中它返回元素的索引和索引对应的值，在集合中返回 key-value 对。
      例如
      for key, value := range oldMap {
          newMap[key] = value
      }
  3.go的文件大小默认以字节为单位，badger内各个文件大小的单位也是字节
  4.go的结构体类型属于混合类型，其内可以包含值类型也可以包含引用类型
    且当结构体直接相互赋值的时候，其会创建一个全新的结构体，不过需要注意的是如果结构体内有引用类型，那么就会这个引用类型是浅拷贝，但如果是简单的值类型，那就是深拷贝了



# BadgerDB

[![Go Reference](https://pkg.go.dev/badge/github.com/dgraph-io/badger/v4.svg)](https://pkg.go.dev/github.com/dgraph-io/badger/v4)
[![Go Report Card](https://goreportcard.com/badge/github.com/dgraph-io/badger/v4)](https://goreportcard.com/report/github.com/dgraph-io/badger/v4)
[![Sourcegraph](https://sourcegraph.com/github.com/dgraph-io/badger/-/badge.svg)](https://sourcegraph.com/github.com/dgraph-io/badger?badge)
[![ci-badger-tests](https://github.com/dgraph-io/badger/actions/workflows/ci-badger-tests.yml/badge.svg)](https://github.com/dgraph-io/badger/actions/workflows/ci-badger-tests.yml)
[![ci-badger-bank-tests](https://github.com/dgraph-io/badger/actions/workflows/ci-badger-bank-tests.yml/badge.svg)](https://github.com/dgraph-io/badger/actions/workflows/ci-badger-bank-tests.yml)
[![ci-golang-lint](https://github.com/dgraph-io/badger/actions/workflows/ci-golang-lint.yml/badge.svg)](https://github.com/dgraph-io/badger/actions/workflows/ci-golang-lint.yml)


![Badger mascot](images/diggy-shadow.png)

BadgerDB is an embeddable, persistent and fast key-value (KV) database written
in pure Go. It is the underlying database for [Dgraph](https://dgraph.io), a
fast, distributed graph database. It's meant to be a performant alternative to
non-Go-based key-value stores like RocksDB.

## Project Status

Badger is stable and is being used to serve data sets worth hundreds of
terabytes. Badger supports concurrent ACID transactions with serializable
snapshot isolation (SSI) guarantees. A Jepsen-style bank test runs nightly for
8h, with `--race` flag and ensures the maintenance of transactional guarantees.
Badger has also been tested to work with filesystem level anomalies, to ensure
persistence and consistency. Badger is being used by a number of projects which
includes Dgraph, Jaeger Tracing, UsenetExpress, and many more.

The list of projects using Badger can be found [here](#projects-using-badger).

Badger v1.0 was released in Nov 2017, and the latest version that is data-compatible
with v1.0 is v1.6.0.

Badger v2.0 was released in Nov 2019 with a new storage format which won't
be compatible with all of the v1.x. Badger v2.0 supports compression, encryption and uses a cache to speed up lookup.

Badger v3.0 was released in January 2021.  This release improves compaction performance.

Please consult the [Changelog] for more detailed information on releases.

For more details on our version naming schema please read [Choosing a version](#choosing-a-version).

[Changelog]:https://github.com/dgraph-io/badger/blob/main/CHANGELOG.md

## Table of Contents
- [BadgerDB](#badgerdb)
  - [Project Status](#project-status)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Installing](#installing)
      - [Installing Badger Command Line Tool](#installing-badger-command-line-tool)
      - [Choosing a version](#choosing-a-version)
  - [Badger Documentation](#badger-documentation)
  - [Resources](#resources)
    - [Blog Posts](#blog-posts)
  - [Design](#design)
    - [Comparisons](#comparisons)
    - [Benchmarks](#benchmarks)
  - [Projects Using Badger](#projects-using-badger)
  - [Contributing](#contributing)
  - [Contact](#contact)

## Getting Started

### Installing
To start using Badger, install Go 1.21 or above. Badger v3 and above needs go modules. From your project, run the following command

```sh
$ go get github.com/dgraph-io/badger/v4
```
This will retrieve the library.

#### Installing Badger Command Line Tool

Badger provides a CLI tool which can perform certain operations like offline backup/restore.  To install the Badger CLI,
retrieve the repository and checkout the desired version.  Then run

```sh
$ cd badger
$ go install .
```
This will install the badger command line utility into your $GOBIN path.

#### Choosing a version

BadgerDB is a pretty special package from the point of view that the most important change we can
make to it is not on its API but rather on how data is stored on disk.

This is why we follow a version naming schema that differs from Semantic Versioning.

- New major versions are released when the data format on disk changes in an incompatible way.
- New minor versions are released whenever the API changes but data compatibility is maintained.
 Note that the changes on the API could be backward-incompatible - unlike Semantic Versioning.
- New patch versions are released when there's no changes to the data format nor the API.

Following these rules:

- v1.5.0 and v1.6.0 can be used on top of the same files without any concerns, as their major
 version is the same, therefore the data format on disk is compatible.
- v1.6.0 and v2.0.0 are data incompatible as their major version implies, so files created with
 v1.6.0 will need to be converted into the new format before they can be used by v2.0.0.
 - v2.x.x and v3.x.x are data incompatible as their major version implies, so files created with
 v2.x.x will need to be converted into the new format before they can be used by v3.0.0.


For a longer explanation on the reasons behind using a new versioning naming schema, you can read
[VERSIONING](VERSIONING.md).

## Badger Documentation

Badger Documentation is available at https://dgraph.io/docs/badger

## Resources

### Blog Posts
1. [Introducing Badger: A fast key-value store written natively in
Go](https://open.dgraph.io/post/badger/)
2. [Make Badger crash resilient with ALICE](https://open.dgraph.io/post/alice/)
3. [Badger vs LMDB vs BoltDB: Benchmarking key-value databases in Go](https://open.dgraph.io/post/badger-lmdb-boltdb/)
4. [Concurrent ACID Transactions in Badger](https://open.dgraph.io/post/badger-txn/)

## Design
Badger was written with these design goals in mind:

- Write a key-value database in pure Go.
- Use latest research to build the fastest KV database for data sets spanning terabytes.
- Optimize for SSDs.

Badger’s design is based on a paper titled _[WiscKey: Separating Keys from
Values in SSD-conscious Storage][wisckey]_.

[wisckey]: https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf

### Comparisons
| Feature                        | Badger                                     | RocksDB                       | BoltDB    |
| -------                        | ------                                     | -------                       | ------    |
| Design                         | LSM tree with value log                    | LSM tree only                 | B+ tree   |
| High Read throughput           | Yes                                        | No                            | Yes       |
| High Write throughput          | Yes                                        | Yes                           | No        |
| Designed for SSDs              | Yes (with latest research <sup>1</sup>)    | Not specifically <sup>2</sup> | No        |
| Embeddable                     | Yes                                        | Yes                           | Yes       |
| Sorted KV access               | Yes                                        | Yes                           | Yes       |
| Pure Go (no Cgo)               | Yes                                        | No                            | Yes       |
| Transactions                   | Yes, ACID, concurrent with SSI<sup>3</sup> | Yes (but non-ACID)            | Yes, ACID |
| Snapshots                      | Yes                                        | Yes                           | Yes       |
| TTL support                    | Yes                                        | Yes                           | No        |
| 3D access (key-value-version)  | Yes<sup>4</sup>                            | No                            | No        |

<sup>1</sup> The [WISCKEY paper][wisckey] (on which Badger is based) saw big
wins with separating values from keys, significantly reducing the write
amplification compared to a typical LSM tree.

<sup>2</sup> RocksDB is an SSD optimized version of LevelDB, which was designed specifically for rotating disks.
As such RocksDB's design isn't aimed at SSDs.

<sup>3</sup> SSI: Serializable Snapshot Isolation. For more details, see the blog post [Concurrent ACID Transactions in Badger](https://blog.dgraph.io/post/badger-txn/)

<sup>4</sup> Badger provides direct access to value versions via its Iterator API.
Users can also specify how many versions to keep per key via Options.

### Benchmarks
We have run comprehensive benchmarks against RocksDB, Bolt and LMDB. The
benchmarking code, and the detailed logs for the benchmarks can be found in the
[badger-bench] repo. More explanation, including graphs can be found the blog posts (linked
above).

[badger-bench]: https://github.com/dgraph-io/badger-bench

## Projects Using Badger
Below is a list of known projects that use Badger:

* [Dgraph](https://github.com/dgraph-io/dgraph) - Distributed graph database.
* [Jaeger](https://github.com/jaegertracing/jaeger) - Distributed tracing platform.
* [go-ipfs](https://github.com/ipfs/go-ipfs) - Go client for the InterPlanetary File System (IPFS), a new hypermedia distribution protocol.
* [Riot](https://github.com/go-ego/riot) - An open-source, distributed search engine.
* [emitter](https://github.com/emitter-io/emitter) - Scalable, low latency, distributed pub/sub broker with message storage, uses MQTT, gossip and badger.
* [OctoSQL](https://github.com/cube2222/octosql) - Query tool that allows you to join, analyse and transform data from multiple databases using SQL.
* [Dkron](https://dkron.io/) - Distributed, fault tolerant job scheduling system.
* [smallstep/certificates](https://github.com/smallstep/certificates) - Step-ca is an online certificate authority for secure, automated certificate management.
* [Sandglass](https://github.com/celrenheit/sandglass) - distributed, horizontally scalable, persistent, time sorted message queue.
* [TalariaDB](https://github.com/grab/talaria) - Grab's Distributed, low latency time-series database.
* [Sloop](https://github.com/salesforce/sloop) - Salesforce's Kubernetes History Visualization Project.
* [Usenet Express](https://usenetexpress.com/) - Serving over 300TB of data with Badger.
* [gorush](https://github.com/appleboy/gorush) - A push notification server written in Go.
* [0-stor](https://github.com/zero-os/0-stor) - Single device object store.
* [Dispatch Protocol](https://github.com/dispatchlabs/disgo) - Blockchain protocol for distributed application data analytics.
* [GarageMQ](https://github.com/valinurovam/garagemq) - AMQP server written in Go.
* [RedixDB](https://alash3al.github.io/redix/) - A real-time persistent key-value store with the same redis protocol.
* [BBVA](https://github.com/BBVA/raft-badger) - Raft backend implementation using BadgerDB for Hashicorp raft.
* [Fantom](https://github.com/Fantom-foundation/go-lachesis) - aBFT Consensus platform for distributed applications.
* [decred](https://github.com/decred/dcrdata) - An open, progressive, and self-funding cryptocurrency with a system of community-based governance integrated into its blockchain.
* [OpenNetSys](https://github.com/opennetsys/c3-go) - Create useful dApps in any software language.
* [HoneyTrap](https://github.com/honeytrap/honeytrap) - An extensible and opensource system for running, monitoring and managing honeypots.
* [Insolar](https://github.com/insolar/insolar) - Enterprise-ready blockchain platform.
* [IoTeX](https://github.com/iotexproject/iotex-core) - The next generation of the decentralized network for IoT powered by scalability- and privacy-centric blockchains.
* [go-sessions](https://github.com/kataras/go-sessions) - The sessions manager for Go net/http and fasthttp.
* [Babble](https://github.com/mosaicnetworks/babble) - BFT Consensus platform for distributed applications.
* [Tormenta](https://github.com/jpincas/tormenta) - Embedded object-persistence layer / simple JSON database for Go projects.
* [BadgerHold](https://github.com/timshannon/badgerhold) - An embeddable NoSQL store for querying Go types built on Badger
* [Goblero](https://github.com/didil/goblero) - Pure Go embedded persistent job queue backed by BadgerDB
* [Surfline](https://www.surfline.com) - Serving global wave and weather forecast data with Badger.
* [Cete](https://github.com/mosuka/cete) - Simple and highly available distributed key-value store built on Badger. Makes it easy bringing up a cluster of Badger with Raft consensus algorithm by hashicorp/raft.
* [Volument](https://volument.com/) - A new take on website analytics backed by Badger.
* [KVdb](https://kvdb.io/) - Hosted key-value store and serverless platform built on top of Badger.
* [Terminotes](https://gitlab.com/asad-awadia/terminotes) - Self hosted notes storage and search server - storage powered by BadgerDB
* [Pyroscope](https://github.com/pyroscope-io/pyroscope) - Open source continuous profiling platform built with BadgerDB
* [Veri](https://github.com/bgokden/veri) - A distributed feature store optimized for Search and Recommendation tasks.
* [bIter](https://github.com/MikkelHJuul/bIter) - A library and Iterator interface for working with the `badger.Iterator`, simplifying from-to, and prefix mechanics.
* [ld](https://github.com/MikkelHJuul/ld) - (Lean Database) A very simple gRPC-only key-value database, exposing BadgerDB with key-range scanning semantics.
* [Souin](https://github.com/darkweak/Souin) - A RFC compliant HTTP cache with lot of other features based on Badger for the storage. Compatible with all existing reverse-proxies.
* [Xuperchain](https://github.com/xuperchain/xupercore) - A highly flexible blockchain architecture with great transaction performance.
* [m2](https://github.com/qichengzx/m2) - A simple http key/value store based on the raft protocol.
* [chaindb](https://github.com/ChainSafe/chaindb) - A blockchain storage layer used by [Gossamer](https://chainsafe.github.io/gossamer/), a Go client for the [Polkadot Network](https://polkadot.network/).
* [vxdb](https://github.com/vitalvas/vxdb) - Simple schema-less Key-Value NoSQL database with simplest API interface.
* [Opacity](https://github.com/opacity/storage-node) - Backend implementation for the Opacity storage project
* [Vephar](https://github.com/vaccovecrana/vephar) - A minimal key/value store using hashicorp-raft for cluster coordination and Badger for data storage.
* [gowarcserver](https://github.com/nlnwa/gowarcserver) - Open-source server for warc files. Can be used in conjunction with pywb
* [flow-go](https://github.com/onflow/flow-go) - A fast, secure, and developer-friendly blockchain built to support the next generation of games, apps and the digital assets that power them.
* [Wrgl](https://www.wrgl.co) - A data version control system that works like Git but specialized to store and diff CSV.
* [Loggie](https://github.com/loggie-io/loggie) - A lightweight, cloud-native data transfer agent and aggregator.
* [raft-badger](https://github.com/rfyiamcool/raft-badger) - raft-badger implements LogStore and StableStore Interface of hashcorp/raft. it is used to store raft log and metadata of hashcorp/raft.
* [DVID](https://github.com/janelia-flyem/dvid) - A dataservice for branched versioning of a variety of data types. Originally created for large-scale brain reconstructions in Connectomics.
* [KVS](https://github.com/tauraamui/kvs) - A library for making it easy to persist, load and query full structs into BadgerDB, using an ownership hierarchy model.
* [LLS](https://github.com/Boc-chi-no/LLS) - LLS is an efficient URL Shortener that can be used to shorten links and track link usage. Support for BadgerDB and MongoDB. Improved performance by more than 30% when using BadgerDB
* [lakeFS](https://github.com/treeverse/lakeFS) - lakeFS is an open-source data version control that transforms your object storage to Git-like repositories. lakeFS uses BadgerDB for its underlying local metadata KV store implementation.

If you are using Badger in a project please send a pull request to add it to the list.

## Contributing

If you're interested in contributing to Badger see [CONTRIBUTING](./CONTRIBUTING.md).

## Contact
- Please use [Github issues](https://github.com/dgraph-io/badger/issues) for filing bugs.
- Please use [discuss.dgraph.io](https://discuss.dgraph.io) for questions, discussions, and feature requests.
- Follow us on Twitter [@dgraphlabs](https://twitter.com/dgraphlabs).

