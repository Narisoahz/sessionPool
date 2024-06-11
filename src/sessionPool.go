package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"strconv"
	"time"
)

type Session struct {
	ID       int
	Expires  int
	UserID   int
	Count    int
	MaxCount int
}

func initialPool() {
	conn, err := newRedisClient()
	if err != nil {
		fmt.Println("Error connecting", err)
		return
	}
	defer conn.Close()

	//初始化session pool
	sessions := make([]*Session, 20)
	for i := 0; i < 20; i++ {
		rand.Seed(time.Now().UnixNano())
		sessions[i] = &Session{
			ID:       i,
			Expires:  rand.Intn(100),
			UserID:   rand.Intn(100),
			Count:    0,
			MaxCount: rand.Intn(9) + 1,
		}
	}
	err1 := initializeSessionPool(conn, sessions)
	if err1 != nil {
		fmt.Println("Error initializing session pool", err1)
		return
	}
	fmt.Println("Session pool initialized")
}

func newRedisClient() (redis.Conn, error) {
	return redis.Dial("tcp", "127.0.0.1:6379")
}

// 初始化session pool
func initializeSessionPool(conn redis.Conn, sessions []*Session) error {
	for _, session := range sessions {
		sessionID := session.ID
		_, err := conn.Do("MULTI")
		if err != nil {
			return err
		}
		conn.Send("RPUSH", "session_pool", sessionID)
		conn.Send("HSET", "sessionId_"+strconv.Itoa(sessionID), "UserID", session.UserID, "Count", session.Count, "MaxCount", session.MaxCount)
		conn.Send("EXPIRE", "sessionId_"+strconv.Itoa(sessionID), session.Expires)
		_, err4 := conn.Do("EXEC")
		if err4 != nil {
			return err4
		}
	}
	return nil
}
