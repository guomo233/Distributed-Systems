package mr

import (
	"log"
	"os"
)

var logFilename string = "mr.log"

func init() {
	f, err := os.Open(logFilename)
	if os.IsNotExist(err) {
		f, err = os.Create(logFilename)
		// TODO handle err
	}
	log.SetOutput(f)
	// TODO flush before exit
}