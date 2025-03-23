package main

import (
	bitcaskkv "bitcask-go"
	bitcaskkv_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number  of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{

	/* string */
	"set": set,
	"get": get,
	"del": del,

	/* hash */
	"hset": hset,
	"hget": hget,
	"hdel": hdel,

	/* set */
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,

	/* list */
	"lpush": lpush,
	"rpush": rpush,
	"lpop":  lpop,
	"rpop":  rpop,

	/* zset */
	"zadd":   zadd,
	"zscore": zscore,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcaskkv_redis.RedisDataStructure
}

// 处理传递过来的具体命令，进行解析并且返回处理结果
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {

	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "'")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcaskkv.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	// 取出参数 set key1 value1
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	// 取出参数 get key
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}

	return value, nil
}

func del(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("del")
	}

	// 由于 Del 是直接调用 Delete 接口的，所以需要先判断是否存在该数据
	// 先判断该 key 是否存在
	var ok = 0
	key := args[0]
	_, err := cli.db.Get(key)
	if err != nil {
		if err == bitcaskkv.ErrKeyNotFound {
			return redcon.SimpleInt(ok), nil
		}
		return nil, err
	}

	// 取出参数 set key1 value1
	err = cli.db.Del(key)
	if err != nil {
		return nil, err
	}
	ok = 1

	return redcon.SimpleInt(ok), nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// HSET key field value
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// HGET key field
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hget")
	}

	key, field := args[0], args[1]
	value, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func hdel(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// HDEL key field
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hdel")
	}

	key, field := args[0], args[1]
	ok, err := cli.db.HDel(key, field)
	if err != nil {
		return nil, err
	}

	// 数据不存在
	if !ok {
		return redcon.SimpleInt(0), nil
	}

	return redcon.SimpleInt(1), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// 往集合key中存入元素，元素存在则忽略，若key不存在则新建
	// SADD key member
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// 判断 member元素是否存在于集合key中
	// SISMEMBER key member
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sismember")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// 从集合key中删除元素
	// SREM key member
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("srem")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// LRUSH key value
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// RRUSH key value
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// LPOP key
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("lpop")
	}

	key := args[0]
	res, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// LPOP key
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("rpop")
	}

	key := args[0]
	res, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {

	// ZSCORE key member
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zscore")
	}

	key, member := args[0], args[1]
	score, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	return score, nil
}
