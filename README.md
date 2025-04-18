## ZZL写在最前：
### (1)目前badger的大致优化方向：
  - 1.热点识别与更新（利用有限的缓存进行更好的冷热分离）  
    如利用逻辑回归或者线性回归之类的简单机器学习方法，对底层数据进行冷热分离，有点类似老师的那个保持高层图数据库查询信息来进行预调取  
    或者在底层利用线性回归和逻辑回归对一些kv对进行聚类，比如要取一个人的信息，通常其所有信息都会取出来，把这些信息通过线性回归或者逻辑回归，全放在一个块  
  - 2.动态参数的调整（BadgerDB的参数目前是锁死的）  
  - 3.（废弃）BadgerDB缓存整个LSM树的每个块，其内会有小V的KV对也会有大V的K OFFSET对  
    能否再开辟一块缓存，然后分成一个有效池，一个无效池，GC的时候用  
    日志合并时记录失效的最大版本。  
    查询或者新增时记录有效的最大版本，有效的不行！！！，因为GC的时候还要往LSM回写OFFSET  
    现在有个比较可行的就是每个vlog文件附带一个失效池，其在日志合并的时候记录（现在貌似也不可行，因为有个概念说BadgerDB的LSM因为太小了，所以可以直接完全的放在内存，那么这样做的话反而会变慢）   

  - 4.TODO:两个缓存池，然后共享同一片区域，但是其各自的比例按实际运行中的特征来变化  
  - 5.TODO:将BLOCK的大小设置为页大小，是否可行呢？  
  - 6.下面是从官网摘抄的  
    - 6.1 范围迭代速度  
      虽然可以以极快的速度进行仅键迭代，但当它需要执行值查找时，速度会变慢。理论上，这种情况不应该发生。亚马逊的 i3.large 磁盘优化实例每秒可以执行 100,000 次 4KB 块随机读取。基于此，我们应该能够每秒迭代 100K 个键值对，换句话说，每分钟六百万个键值对。  
      然而，Badger 当前的实现无法产生接近这个限制的 SSD 随机读取请求，因此键值迭代也受到了影响。在这个空间中还有很大的优化空间。  

    - 6.2 压缩速度  
      与 RocksDB 相比，Badger 在运行压缩操作时速度较慢。因此，对于仅包含较小值的纯数据集，加载到 Badger 中的数据会更慢。这需要更多的优化。  

    - 6.3 LSM 树压缩  
      在一个仅包含较小值的数据集中，LSM 树的尺寸将比 RocksDB 大得多，因为 Badger 没有在 LSM 树上运行压缩。如果需要，这应该很容易添加，并且会是一个极佳的初次贡献项目。  

    - 6.4 B+树方法  
      近年来 SSD 的改进可能使 B+树成为可行的选择。自从 WiscKey 论文发表以来，SSD 在随机写入性能方面取得了巨大进步。一个新的有趣方向是将值日志方法与仅保留键和值指针的 B+树相结合。这将用 LSM 树读排序合并的顺序写入压缩与每个键更新的许多随机写入进行交换，可能实现与 LSM 相同的写入吞吐量，但设计更为简单。  

### (2)BadgerDB的一些需要关注点
  - 1.SST分为元数据块，索引块，DATA块  
  - 2.目录中文件名后面的带test的，是做单元测试的模块  
  - 3.有两级缓存，一级索引的缓存，一个块的缓存（详见7）  
  - 4.数据可能在内存，也可能在外存，也可能在事务中，共三类。而对这三类查找时，每类均会有一个迭代器，每个迭代器的实现方式也不一样  
  - 5.badger 中与事务相关的结构体包括 Txn 和 oracle 两个  
    单个事务层面，在 Txn 结构体中，需要记录自己读写的 key 列表，以及事务的开始时间戳（readTs，用于读取）和提交时间戳（commitTs，用于在commit的时候加在写入的key后，注意这两个时间戳全是由o.nextTxnTs产生），以及读写的 key 列表。  
    全局事务层面，在 oracle 结构体中，其相当于相当于事务管理器，需要管理全局时间戳，以及近期提交的事务列表（committedTxns），用于在新的事务提交中对事务开始与提交时间戳中间提交过的事务范围进行冲突检查，以及当前活跃的事务的最小时间戳，用于清理旧事务信息（指清理committedTxns数组，这个数组增加时是在newCommitTs的时候增加）。  
    而WaterMark（水位） 结构体内部是个堆，用于管理、查找事务开始、结束的区段。oracle 的 txnMark 主要用于协调等待 Commit 授时与落盘的时间窗口，readMark 管理当前活跃事务的最早时间戳，用于清理过期的 committedTxns（在cleanupCommittedTransactions函数中，触发清理的有两处，可以搜o.cleanupCommittedTransactions()来查看）。    
    - 5.1 事务时间戳是逻辑时间戳，每次事务提交时递增 1。  
    - 5.2 只读事务是不进行提交操作的，所以没有commitTs。写入数据是在获取commitTs之后，读取应该是在获取commitTs之前.  
    - 5.3 TODO: 那个事务过程笔记中，如果事务4是写入A事务，3是进行读取A的事务，会怎样（即写后读会咋样）  
    - 5.4 SSI 事务中冲突探测的逻辑就是，当某一个事务commit的时候，找出在当前事务执行期间 Commit 的事务列表，检查当前事务读取的 key 列表是否与这些事务的写入的 key 列表有重叠。  

  - 6.levelHandler中存的SST表，即tables存储的是那个SST的所有数据吗，还是只有一些关键信息？  
    不是，里面有各个块的信息（这个信息貌似是以Mmap方式存储在内存的），找目标key先找到目标块，然后再看这个块是否在cache中，不在的话去磁盘找  
  
  - 7.缓存在哪里用的？
    - 7.1 首先，两个缓存都在db初始化的时候开辟出来并把缓存对象存储在db中，对象名字分别为 db.indexCache db.blockCache  
      db.blockCache ，其是在迭代器的对象内使用以及增加块缓存（badger操作的基本单位） ，分别在NOTE:420  以及  NOTE:421处  
      db.indexCache ，缓存的是SST的索引块（就类似开一个元数据的缓存），目前发现用来做布隆过滤器的过滤用,NOTE:422,  其内还会有当前SST中的过时数据大小 NOTE:423  
    - 7.2 db.blockCache增加以及查询的时机在  
      查询完memtable以及immemtable后未找到，遍历层去找，每层找出可能存在目标K的SST（注意在此会用布隆），然后再在SST内查找（得到目标块的idx），再依据目标块的idx来查缓存以及添加缓存  

  - 8.mmap具体在badger如何实现？（看ristretto怎么用即可）  
    - 8.1 ristretto实现mmap时，调用的是共用的系统调用库syscall中的函数    
    且mmap在GO中。对于linux以及win的实现不一样，详见https://www.cnblogs.com/larkwins/p/17737296.html  
    - 8.2 目前使用ristretto的Msync函数来进行手动同步（如z.Msync(mf.Data)，mf.Data是内存映射的那个内存空间），且需要注意的是貌似win的手动同步函数并未在ristretto实现，其仍是空的函数体的TODO  
    - 8.3 mmap映射区域大小必须是物理页大小(page_size)的整倍数（32位系统中通常是4k字节）。  
    - 8.4 内核可以跟踪被内存映射的底层对象（文件）的大小，进程可以合法的访问在当前文件大小以内又在内存映射区以内的那些字节。也就是说，如果文件的大小一直在扩张，只要在映射区域范围内的数据，进程都可以合法得到，这和映射建立时文件的大小无关。  

  - 9.GC按理来说应该是定期会自动检查的（可能在日志合并的时候？），难道是需要自己去手动调RunValueLogGC这个函数吗？  
    Badger的大value是存放在value log文件中, 它很聪明的一点是GC 接口只交给用户来调度, 而不是自己内部自主触发, 这样的责任划分就非常清晰了, 用户自己选择开启关闭GC, 来自己承担GC引入的读写问题, 真是机智。所以就是让自己去调用的，这样用户就可以安排在自己的空闲时间来进行GC操作  
  
  - 10.目前badger主要会有4类压缩，L0->L0,LMAX->LMAX,L0->Lbase(基线层)还有普通的Li-1到Li  
  
  - 11.K与V的大小比例一般是多少  
    键值比：通常键占 1%-5%，值占 95%-99%  
  
  - 12.每层的目标空间大小是依据最底层实时计算的，那么最底层的怎么加大小的？  
    2号合并协程定期执行最底层合并时，此时一般会增加，还有倒二到最底合并也会增加  
  
  - 13.badger的那个迭代器是怎么具体实现的？  
    子压缩的迭代器 （NOTE:470） 以及 样例代码的那个遍历迭代器（NOTE:471）均是如下实现    
    PS:y.Iterator是最基础的迭代器对象接口    
    - 13.1 一般先通过newIterator等很多种类的函数创建不同种的迭代器子对象（实现y.Iterator）这个对象内会按照自身的特性各自实现Iterator接口中的Next()，Rewind()，Seek()等各个函数，然后再把这个需要迭代的子迭代器对象放切片里面
    - 13.2 再执行NewMergeIterator函数将上述切片合成一个总的迭代器对象（实现y.Iterator），该总迭代器对象是一个平衡二叉树，叶节点为迭代器子对象（实现y.Iterator），非叶的是MergeIterator对象（实现y.Iterator，其内也会实现Next()，Rewind()，Seek()等函数）  
    - 13.3 调用MergeIterator对象的各个函数时，便会逐级调用二叉树至底层的叶子节点  
### (3)一些简称
  lc levercontroler 层级管理器  
  lf logFile log文件对象  
  vp valuePointer 值指针  

### (4)注意Badger 本身不进行分发，Dgraph 在其之上实现了一层来提供分布式功能。

### (5)mmap是什么？及其特性，关键点 
  PS:（BadgerDB内也使用了mmap，如Table,discardStats,logFile等都是由mmap实现的，可以搜索MmapFile来定位，MmapFile结构体内会有有个内存的字节切片对象Data以及一个外存的文件对象Fd）  
  详见 https://zhuanlan.zhihu.com/p/874373786  
  首先，传统的IO模型进行磁盘数据读写时，一般大致需要2个步骤，拿写入数据为例：1.从用户空间拷贝到内核空间；2.从内核空间写入磁盘。    
  - 1.mmap（memory mapped file）是一种内存映射文件的方法，即将一个文件或者其它对象映射到进程的地址空间，实现文件磁盘地址和进程虚拟地址空间中一段虚拟地址的一一对映关系.    
  内存映射是一种将文件的部分或全部内容映射到进程的虚拟内存空间的技术。在 Linux 系统中，通过mmap函数来实现。当进行内存映射时，操作系统会在物理内存中为文件内容分配页面，并将这些页面与进程的虚拟地址空间建立关联。这个关联的过程使得进程可以像访问内存一样访问文件内容，而不需要通过传统的文件读取（如read函数）和写入（如write函数）操作。  
  从硬件层面看，内存映射利用了 CPU 的内存管理单元（MMU）。MMU 负责将虚拟地址转换为物理地址。当进程访问已映射的虚拟内存地址时，MMU 会将这个访问请求转换为对实际物理内存中文件数据所在页面的访问，这个过程对应用程序是透明的。如果访问的数据不在物理内存中（产生缺页中断），操作系统会从磁盘文件中将相应的数据页面加载到物理内存。    
  与传统IO模式相比，减少了一次用户态copy到内核态的操作！！！也因此认为其比较快。    
  - 2.mmap 由操作系统负责管理，对同一个文件地址的映射将被所有线程共享，操作系统确保线程安全以及线程可见性；  
  - 3.需要注意的是，内存映射并不要求一次性将整个文件加载到内存中。且不会在创建mmap时，就把所有相关页面放到内存，操作系统会根据实际的访问情况（如当进程发起读或写操作时），自动将需要的文件部分加载到内存。    
  当进行IO操作时，发现用户空间内不存在对应数据页时（缺页），会先到交换缓存空间（swap cache）去读取，如果没有找到再去磁盘加载（调页）。  
  - 4.且特别注意，如果写操作改变了其内容，一定时间后系统会自动回写脏页面到对应磁盘地址，也即完成了写入到文件的过程。  
    即修改过的脏页面并不会立即更新回文件中，而是有一段时间的延迟，可以调用 msync() 来强制同步, 这样所写的内容就能立即保存到文件里了。  
  - 5.其可以用在哪里  
    进程间通信：从自身属性来看，Mmap具有提供进程间共享内存及相互通信的能力，各进程可以将自身用户空间映射到同一个文件的同一片区域，通过修改和感知映射区域，达到进程间通信和进程间共享的目的。  
    大数据高效存取：对于需要管理或传输大量数据的场景，内存空间往往是够用的，这时可以考虑使用Mmap进行高效的磁盘IO，弥补内存的不足。例如RocketMQ，MangoDB等主流中间件中都用到了Mmap技术；总之，但凡需要用磁盘空间替代内存空间的时候都可以考虑使用Mmap。  


### (6)MVCC是什么？（在BadgerDB中用MVCC方式做事务处理）
  - 1.MVCC 英文全称叫 "Multi Version Concurrency Control"，翻译过来就是 "多版本并发控制"。在 MySQL 众多存储引擎中只有 InnoDB 存储引擎中实现了 MVCC 机制。MVCC是快照读，非普通的当前读  
  - 2.简单来说的话，就是每个数据（BadgerDB中的每个KV对）都有多个版本（与LSM树特性一致），然后这些版本会构成一个版本链（undo log），这条链中，每个版本中会存一个trx_id(导致当前版本产生的事务id) 和roll_pointer(回滚指针，指向上一个版本)。然后决定读取操作数据哪个版本的，就取决于readView，其是事务在使用 MVCC 机制对数据库中的数据操作时产生的读视图，即当事务开启之后，会生成数据库当前系统的一个快照（这个快照最核心的是构造一个数据结构，包含一个数组（当前系统活跃事务的id，活跃指的是正在操作，但是还未提交的事务）与该数组中的最小ID值与预分配的事务ID（下一个事务ID）与创建该快照的事务ID。值得注意的是，每个快照会对应这样一个数据结构，而这个数据结构也正是处理事务处理时序问题的关键（貌似BadgerDB会在每个数据后拼接上时间戳来当版本号，然后每个数据的快照版本，就可以用这个时间戳来实现，具体的还需要更详细看代码！！！！！）
  而readView 的核心原理主要体现在 READ COMMITTD(读已提交)和 REPEATABLE READ(可重复读) 两种隔离级别上。（共有四种隔离级别，如下  
                读未提交：解决了脏写问题；  
                读已提交：解决了脏写，脏读问题；  
                可重复读：解决了脏写，脏读，不可重复读；  
                串行化：解决了脏写，脏读，不可重复读，幻读所有问题；）  
  READ COMMITTD：在每次进行 SELECT 查询操作的时候都会去生成一个 readView；  
  REPEATABLE READ：每开启一个事务才会生成一个 readView，一个事务的所有SQL语句共享一个 readView。（一般都是这个级别实现的，在这种情况下，相当于每一个事务在创建的时候都会存一个上面提到的那个快照的活跃事务ID数组，并以此来判断事务可以用到哪些数据，比如如果数据上附加的版本号小于快照中的min_trx_id，那么就可以用）  
  一定一定要区分好读已提交和可重复读隔离级别下 readView 的生成时机，这是这两种隔离级别原理最最最核心的因素。再次强调！！！  
  PS：在 InnoDB 存储引擎中，幻读的问题也得到了解决，解决的方式是利用间隙锁；  
  PS：幻读与不可重复读都属于并发引发的问题，都指的是一个事务两次查询结果不一样，但是幻读通常针对整张表（指如整张表的统计数据），而不可重复读一般针对的是某一条数据或者某几条数据（即确切的某条数据）。  


### (7)badger具体实现了哪种隔离
  - 1.badger实现了可串行的快照隔离 (Serializable Snapshot Isolation, SSI)  
  - 2.SSI是一种乐观的并发控制机制。它的原则是事情可能导致出错，但是不管它，所有事务都继续执行，SSI希望最后能得到正确的结果。所以让大家都直接执行，直到事务结束之后，SSI去检查是否结果有并发问题。如果有，那就回滚这个事务。  
  - 3.所以，SSI (可串行的快照隔离) 比起之前的快照隔离，区别在于：SSI多了检测串行化冲突和决定哪些事务需要回滚的机制。  
  详见 https://zhuanlan.zhihu.com/p/395229054  

### (8)GO语言
  - 1.<- chan 如果取不出来数据，会把当前执行环境阻塞的  
  - 2.for range用于 for 循环中迭代数组(array)、切片(slice)、通道(channel)或集合(map)的元素。在数组和切片中它返回元素的索引和索引对应的值，在集合中返回 key-value 对。  
      例如  
      for key, value := range oldMap {  
          newMap[key] = value  
      }  
  - 3.go的文件大小默认以字节为单位，badger内各个文件大小的单位也是字节  
  - 4.go的结构体类型属于混合类型，其内可以包含值类型也可以包含引用类型    
    且当结构体直接相互赋值的时候，其会创建一个全新的结构体，不过需要注意的是如果结构体内有引用类型，那么就会这个引用类型是浅拷贝，但如果是简单的值类型，那就是深拷贝了  

### (9)Ristretto 是badger官方自己实现的缓存库，用于Badger以及Dgraph  
  - 1.首先，其是一个非持久化的，非分布式的，通过 TinyLFU论文 实现准入策略，且替换策略使用采样LFU的缓存  
  - 2.认为每一个item都有一个成本，在Set加入的时候就可以指定加入的item的成本，  
  - 3.Ristretto设置缓存总大小，而非设置缓存item的数量  
  - 4.TinyLFU 其是一种与替换策略无关的接纳策略，旨在通过极小的内存开销来提高命中率。主要思想是只允许新项进入，如果其估计值高于被替换项的估计值。Ristretto使用 Count-Min Sketch 在 Ristretto 中实现了 TinyLFU。它使用 4 位计数器来近似项的访问频率（ɛ，进入键的ε值应该高于被替换键的ε值）。每个键的这种小成本使我们能够跟踪比使用正常键到频率映射更大的全局键空间样本。  
    TinyLFU 还通过 Reset 函数维护键访问的最近性。经过 N 次键增加后，计数器减半。因此，一段时间内未出现的键将计数器重置为零；为最近出现的键铺平道路。

### (10)badger与Dgraph均用linux自身的makeFile来进行打包
  详见https://blog.csdn.net/weixin_64132124/article/details/143465619
  PS:进入项目的根目录，然后执行make指令即可打包，会把badger与Dgraph各自的二进制执行程序打包到各自项目目录的同名文件夹下。注意使用make命令之前需要执行makefile文件内的dependence的下载命令  


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

