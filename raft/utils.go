package main

import (
	"hash/fnv"
	"math/rand"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
)

var log = NewLogObject("", logrus.DebugLevel)

func NewLogObject(serverId string, level logrus.Level) *logrus.Logger {
	log := logrus.New()
	log.Level = level
	log.Formatter = new(logrus.TextFormatter)
	return log
}

func parse(args []string) (string, map[string]int) {
	var id string
	var servers = make(map[string]int)
	for i := 0; i < len(args); i++ {
		if args[i] == "-id" {
			id = args[i+1]
			i += 1
		} else if args[i] == "-s" {
			i += 1
			number, _ := strconv.Atoi(args[i])
			i += 1
			for j := 0; j < number; j++ {
				sid := args[i+j*2]
				port, _ := strconv.Atoi(args[i+j*2+1])
				servers[sid] = port
			}
			i += number * 2
		}
	}
	return id, servers
}

func isLaterThan(oldTime, newTime time.Time) bool {
	cmp := compareTime(newTime, oldTime)
	return cmp >= 0
}

func compareTime(t1, t2 time.Time) int {
	return compareInt64(t1.UnixNano(), t2.UnixNano())
}

func compareInt64(v1, v2 int64) int {
	if v1 == v2 {
		return 0
	} else if v1 > v2 {
		return 1
	} else {
		return 2
	}
}

func randomDuration(base int) time.Duration {
	dr := time.Duration(base + rand.Intn(300))
	return dr
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func min(v1, v2 uint32) uint32 {
	if v1 > v2 {
		return v2
	}
	return v1
}

func max(v1, v2 uint32) uint32 {
	if v1 > v2 {
		return v1
	}
	return v2
}
