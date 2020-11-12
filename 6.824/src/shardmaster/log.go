package shardmaster

import (
	"log"
	"os"
)

var logFilename string = "/dev/null"

func init() {
	f, _ := os.Create(logFilename)
	// TODO handle err
	log.SetOutput(f)
	// TODO flush before exit
}