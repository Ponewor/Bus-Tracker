package main

import (
	"encoding/json"
	"fmt"
	"github.com/asmarques/geodist"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var apiKey = "d561bd20-5bcf-4ce6-be8a-abcbb700ffdd"

var url1 = "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=b27f4c17-5c50-4a5b-89dd-236b282bc499&name=nazwaprzystanku&apikey=d561bd20-5bcf-4ce6-be8a-abcbb700ffdd"

var url2 = "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=wartość&busstopNr=wartość&apikey=d561bd20-5bcf-4ce6-be8a-abcbb700ffdd"

var url3 = "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId=wartość&busstopNr=wartość&line=wartość&apikey=d561bd20-5bcf-4ce6-be8a-abcbb700ffdd"

type Response1 struct {
	Result []struct {
		Values []struct {
			Value string `json:"value"`
			Key   string `json:"key"`
		} `json:"values"`
	} `json:"result"`
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

	var responseObject Response1
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

func getLines(stopId string, stopNumber string) []string {
	address := "https://api.um.warszawa.pl/api/action/dbtimetable_get?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=" + stopId + "&busstopNr=" + stopNumber + "&apikey=" + apiKey
	r, err := http.Get(address)
	if err != nil {
		panic(err)
	}

	responseData, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	var responseObject Response1
	err = json.Unmarshal(responseData, &responseObject)
	if err != nil {
		panic(err)
	}

	lines := make([]string, len(responseObject.Result))
	for i, lineEntry := range responseObject.Result {
		lines[i] = lineEntry.Values[0].Value
	}

	return lines
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

var vehiclesPerLines map[string]map[string]KafkaMessage

func handler(w http.ResponseWriter, r *http.Request) {
	s := strings.Split(r.URL.Path[1:], "/")
	if len(s) < 3 {
		return
	}
	line := s[2]

	latitude, _ := strconv.ParseFloat(s[0], 64)
	longitude, _ := strconv.ParseFloat(s[1], 64)

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


func main() {
	vehiclesPerLines = map[string]map[string]KafkaMessage{}
	var message KafkaMessage

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"broker.address.family": "v4",
		"group.id":              "tracker",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.Subscribe("ztm-output", nil)

	run := true

	go func() {
		for run == true {
			select {
			case sig := <-sigchan:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				os.Exit(0)
			default:
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
					// Errors should generally be considered
					// informational, the client will try to
					// automatically recover.
					// But in this example we choose to terminate
					// the application if all brokers are down.
					fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
					if e.Code() == kafka.ErrAllBrokersDown {
						run = false
					}
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}
		}
	}()

	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
