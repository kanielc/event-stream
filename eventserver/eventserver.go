package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var data []string
var dataSize float64

func LoadData(basePath string, filePattern string) error {
	dir, _ := os.ReadDir(basePath)

	for _, file := range dir {
		name := file.Name()
		matched, _ := filepath.Match(filePattern, name)
		if matched {
			contents, err := ioutil.ReadFile(fmt.Sprintf("%s%c%s", basePath, os.PathSeparator, name))
			if err != nil {
				return err
			}

			var jsonElements []map[string]interface{}
			err = json.Unmarshal(contents, &jsonElements)
			if err != nil {
				return err
			}

			for _, value := range jsonElements {
				b, _ := json.Marshal(value)
				data = append(data, string(b))
			}
		}
	}

	dataSize = float64(len(data))
	log.Printf("Loaded %d records to be served over %d seconds, at approximately %d per second", int(dataSize), int(runLength), int(dataSize/runLength))
	return nil
}

var curPos int
var runLength float64
var startTime int

func handler(w http.ResponseWriter, r *http.Request) {
	expected := float64(int(time.Now().Unix())-startTime) / runLength * dataSize
	max := int(math.Min(expected, float64(len(data)-1)))

	// handle needing an empty result
	if curPos == max || max == 0 {
		fmt.Fprintf(w, "")
	} else {
		fmt.Fprintf(w, "%s", strings.Join(data[curPos:max], "\n"))
	}

	//log.Printf("Returning %d records, current index %d, moving towards %d with total uptime of %d and service period of %d", max-curPos, curPos, max, (int(time.Now().Unix()) - startTime), int(runLength))

	curPos = max
}

func main() {
	// load data
	basePath := flag.String("d", ".", "directory of files to load")
	filePattern := flag.String("g", "*.json", "glob pattern of json files to load")
	length := flag.Int("l", 300, "how many seconds data will be served for (at most 1 call after will return results)")
	port := flag.Int("p", 4056, "What port to listen to HTTP requests on.")

	flag.Parse()
	runLength = float64(*length)
	err := LoadData(*basePath, *filePattern)
	CheckError(err)

	http.HandleFunc("/next", handler)
	startTime = int(time.Now().Unix())
	http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
}
