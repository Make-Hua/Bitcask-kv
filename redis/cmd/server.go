package main

import (
	bitcaskkv "bitcask-go"
	bitcaskkv_redis "bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6399"

type BitcaskServer struct {
	dbs    map[int]*bitcaskkv_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {

	// 打开 Redis 数据结构服务
	redisDataStructure, err := bitcaskkv_redis.NewRedisDataStructure(bitcaskkv.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcaskkv_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accapt connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {

	// 初始化客户端
	cli := new(BitcaskClient)

	svr.mu.Lock()
	defer svr.mu.Unlock()

	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)

	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {

	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

/*
// redis 协议解析所需处理事情的简单实例
func main() {

	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}

	// 向 redis 发送一个信息
	cmd := "set key1 bitcaskkv1\r\n"
	conn.Write([]byte(cmd))

	// 解析 redis 响应
	reader := bufio.NewReader(conn)
	res, err := reader.ReadString('\n')
	if err != nil {
		panic(err)
	}
	fmt.Println(res)
}
*/
