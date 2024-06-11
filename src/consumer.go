package main

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"sync"
	"time"
)

var wg sync.WaitGroup
var pool *redis.Pool

func init() {
	pool = &redis.Pool{
		MaxIdle:   3,
		MaxActive: 5,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "localhost:6379")
		},
	}
}

func main() {
	//初始化sessionPool
	initialPool()

	//创建限流通道
	limitGet := make(chan struct{}, 3)
	limitReturn := make(chan struct{}, 3)
	//启动10个消费者goroutine
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			//消费者取出和归还Session
			//可以通过redisPool或者管道实现限流
			for {
				//取出session
				limitGet <- struct{}{}
				session, err := getSession()
				<-limitGet
				if err != nil {
					fmt.Println("Error getting session", err)
					return
				}
				fmt.Println("Consumer: ", id, " get Session: ", session.ID, " count: ", session.Count, " maxCount: ", session.MaxCount)
				// 模拟处理会话
				time.Sleep(5 * time.Second)
				//归还session
				limitReturn <- struct{}{}
				err = returnSession(session)
				<-limitReturn
				if err != nil {
					fmt.Println("Error returning session", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	pool.Close()
}

// 获取队首的Session（没过期且没达到最大使用次数）
func getSession() (*Session, error) {
	conn := pool.Get()
	defer conn.Close()
	var sessionID int
	var sessionIDTemp interface{}
	var result map[string]string
	var err error
	for {
		sessionIDTemp, err = conn.Do("LPOP", "session_pool")
		if err != nil {
			return nil, err
		}
		//list中没有session的情况
		if sessionIDTemp == nil {
			//判断redis中是否还有seesion存在
			remain_num, _ := redis.Int(conn.Do("KEYs", "sessionId_*"))
			if remain_num == 0 {
				//全部session都过期了
				return nil, errors.New("no session remain")
			} else {
				//还有session存在
				continue
			}
		}
		sessionID, err = redis.Int(sessionIDTemp, err)
		//判断session是否过期
		result, err = redis.StringMap(conn.Do("HGETALL", "sessionId_"+strconv.Itoa(sessionID)))
		if err != nil {
			return nil, err
		}
		if len(result) != 0 {
			break
		}
	}
	//找到session
	//给使用次数加1
	count, err := redis.Int(conn.Do("HINCRBY", "sessionId_"+strconv.Itoa(sessionID), "Count", 1))
	if err != nil {
		return nil, err
	}
	session := &Session{
		ID:    sessionID,
		Count: count,
	}
	session.UserID, _ = strconv.Atoi(result["UserID"])
	//判断是否超过最大使用次数
	session.MaxCount, _ = strconv.Atoi(result["MaxCount"])
	if count >= session.MaxCount {
		//删除session
		_, err = conn.Do("del", "sessionId_"+strconv.Itoa(sessionID))
		if err != nil {
			return nil, err
		}
		fmt.Println("delete sessionId_" + strconv.Itoa(sessionID))
	}
	return session, nil
}

// 归还session到队尾
func returnSession(session *Session) error {
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("RPUSH", "session_pool", session.ID)
	return err
}
