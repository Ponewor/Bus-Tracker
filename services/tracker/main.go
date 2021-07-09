package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/asmarques/geodist"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var apiKey = "d561bd20-5bcf-4ce6-be8a-abcbb700ffdd"

type Response struct {
	Result []struct {
		Values []struct {
			Value string `json:"value"`
			Key   string `json:"key"`
		} `json:"values"`
	} `json:"result"`
}

type KafkaMessage struct {
	Lines         string  `json:"lines"`
	Lon           float64 `json:"lon"`
	Lat           float64 `json:"lat"`
	VehicleNumber string  `json:"vehicleNumber"`
	Brigade       string  `json:"brigade"`
	Time          string  `json:"time"`
	Speed         float64 `json:"speed"`
	Distance      float64 `json:"distance"`
	Bearing       float64 `json:"bearing"`
}

func getStopId(stop string) string {
	address := "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=b27f4c17-5c50-4a5b-89dd-236b282bc499&apikey=" + apiKey + "&name=" + url.QueryEscape(stop)
	r, err := http.Get(address)
	if err != nil {
		panic(err)
	}

	responseData, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	var responseObject Response
	err = json.Unmarshal(responseData, &responseObject)
	if err != nil {
		panic(err)
	}

	for _, entry := range responseObject.Result[0].Values {
		if entry.Key == "zespol" {
			return entry.Value
		}
	}

	panic("no return value")
}

func getLines(stopId string, stopNumber string) ([]string, error) {
	address := "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=" + stopId + "&busstopNr=" + stopNumber + "&apikey=" + apiKey
	r, err := http.Get(address)
	if err != nil {
		panic(err)
	}

	responseData, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	var responseObject Response
	err = json.Unmarshal(responseData, &responseObject)
	if err != nil {
		panic(err)
	}

	if len(responseObject.Result) == 0 {
		return nil, errors.New("empty result")
	}
	lines := make([]string, len(responseObject.Result))
	for i, lineEntry := range responseObject.Result {
		lines[i] = lineEntry.Values[0].Value
	}

	return lines, nil
}

func getAllLines(stopId string) []string {
	linesMap := map[string]struct{}{}

	for i := 1; ; i++ {
		lines, err := getLines(stopId, fmt.Sprintf("%02d", i))
		if err != nil {
			break
		}
		for _, line := range lines {
			linesMap[line] = struct{}{}
		}
	}

	allLines := make([]string, 0, len(linesMap))
	for line := range linesMap {
		allLines = append(allLines, line)
	}

	return allLines
}

var vehiclesPerLines map[string]map[string]KafkaMessage

func nearestVehicleHandler(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path[1:], "/")

	if len(s) < 4 {
		return
	}
	line := s[3]

	latitude, _ := strconv.ParseFloat(s[1], 64)
	longitude, _ := strconv.ParseFloat(s[2], 64)

	location := geodist.Point{
		Lat:  latitude,
		Long: longitude,
	}

	bestDistance := math.Inf(1)
	bestVehicle := KafkaMessage{}

	for _, message := range vehiclesPerLines[line] {
		busLocation := geodist.Point{
			Lat:  message.Lat,
			Long: message.Lon,
		}

		distance, _ := geodist.VincentyDistance(location, busLocation)
		if distance < bestDistance {
			bestDistance = distance
			bestVehicle = message
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(bestVehicle)
}

func stopHandler(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path[1:], "/")
	if len(s) < 2 {
		return
	}
	stop := s[1]
	id := getStopId(stop)
	lines := getAllLines(id)

	vehiclesData := []KafkaMessage{}

	for _, line := range lines {
		for _, message := range vehiclesPerLines[line] {
			vehiclesData = append(vehiclesData, message)
		}
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(vehiclesData)
}

func main() {
	vehiclesPerLines = map[string]map[string]KafkaMessage{}
	var message KafkaMessage

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     "kafka:9092",
		"broker.address.family": "v4",
		"group.id":              "tracker",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = c.Subscribe("ztm-output", nil)

	run := true

	go func() {
		for run == true {
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				json.Unmarshal(e.Value, &message)
				if _, ok := vehiclesPerLines[message.Lines]; !ok {
					vehiclesPerLines[message.Lines] = map[string]KafkaMessage{message.VehicleNumber: message}
				} else {
					vehiclesPerLine := vehiclesPerLines[message.Lines]
					if oldMessage, ok := vehiclesPerLine[message.VehicleNumber]; !ok || oldMessage.Time < message.Time {
						vehiclesPerLine[message.VehicleNumber] = message
					}
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}()

	http.HandleFunc("/nearest_vehicle/", nearestVehicleHandler)
	http.HandleFunc("/stop/", stopHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
