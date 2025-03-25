# 基于 bitcask 的 kv 存储引擎

## 项目介绍

介绍：使用 go 语言实现了基于 bitcask 模型的存储引擎，支持内嵌式接口 put / get / delete，同时封装实现 HTTP 接口并且兼容Redis。
- 采用基于 bitcask 的 Key / Value 数据存储模型，实现数据存储和检索的高吞吐量、快速、稳定
- 存储引擎的内存索引进行抽象接口设计并以此基础实现了 BTree、ARTTree、B+tree，提高了内存索引的可扩展性
- 文件 IO 方面实现文件锁保证多进程下的并发安全，同时设计实现了 MMap 提高了存储引擎启动的效率
- 通过使用库 redcon 实现了 Redis 的 RESP 协议，接入了 Redis 的五种基本类型，可通过 redis-cli 连接并使用
- 存储引擎通过实现 WriterBatch 原子写从而实现存储引擎的事务，保证了事务的 ACID 特性
- 存储引擎实现了 HTTP 接口，可通过 HTTP 接口对存储引擎进行访问且调用


## 开发环境

- 操作系统：`Ubuntu 22.04`
- 编译器：`go1.23.5 linux/amd64`
- 版本控制：`git`

## 项目思路及各个模块讲解

#### [Bitcask 详解](https://www.yuque.com/g/makehua/nfertq/lstxskxymrt30c0u/collaborator/join?token=IyQiNiQayz5WOqwY&source=doc_collaborator# 《Bitcask 存储模型》)

#### [Bitcask kv 的代码实现](https://www.yuque.com/g/makehua/nfertq/wr9r8m5rbzi98b1h/collaborator/join?token=YqsxQe3b6ouuN6kw&source=doc_collaborator# 《基于 bitcask 的 kv 存储引擎》)