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

	log.Println(curPos, max, expected, (int(time.Now().Unix()) - startTime), runLength)

	// handle needing an empty result
	if curPos == max || max == 0 {
		fmt.Fprintf(w, "[]")
		log.Printf("Returned 0 records. CurPos is %d\n", curPos)
	} else {
		fmt.Fprintf(w, "[%s]", strings.Join(data[curPos:max], ","))
		log.Printf("Returned %d records\n", max-curPos)
	}

	curPos = max
}

func main() {
	// load data
	basePath := flag.String("location", ".", "directory of files to load")
	filePattern := flag.String("pattern", "*.json", "pattern of json files to load")
	length := flag.Int("length", 300, "how many seconds data will be served for (at most 1 call after will return results)")

	flag.Parse()
	runLength = float64(*length)
	err := LoadData(*basePath, *filePattern)
	CheckError(err)

	http.HandleFunc("/next", handler)
	startTime = int(time.Now().Unix())
	http.ListenAndServe(":8080", nil)
}
