package kvraft

import (
	"log"
	"os"
)

var logFilename string = "kvraft.log"

func init() {
	f, _ := os.Create(logFilename)
	// TODO handle err
	log.SetOutput(f)
	// TODO flush before exit
}