package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
	// "github.com/tierex/conslib/raft"
)

func main() {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)
	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for t := range ticker.C {
			fmt.Println("Tick at", t)
		}
	}()

	time.Sleep(time.Millisecond * 1600)
	ticker.Stop()
	fmt.Println("Ticker stopped")

	// args := os.Args[1:]
	// configFile := dir + "/../config/config.json"
	//
	// if len(args) == 1 {
	// 	configFile = args[0]
	// }
	//
	// config := raft.ReadConfig(configFile)
	// raft.Run(config)
}
