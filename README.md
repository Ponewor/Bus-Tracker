# Bus Tracker

Bus Tracker is an application that lets you monitor Warsaw's public transportation vehicles while you are waiting on a stop.

## Installation

To run the backend side of the app use

`docker-compose up`

The frontend (`services/app/index.html`) can be accessed by web browser.

## Usage

Just type the name of your stop into the input box and after a few seconds you will see vehicles moving on a map.

## How does it work?

There are four components.

- `ztm-hook` is a simple Python script that polls ZTM API and writes vehicles' location data to the topic `ztm-input` run by Kafka
- `kafka-stream` is a Java application using Kafka Streams that reads messages from `ztm-input`, augments them with vehicles' speed and bearing and finally writes to the `ztm-output` topic
- `tracker`is a Go based server side application providing two REST API endpoints to the data collected from `ztm-output`
- `app` is basic client side JavaScript application showing vehicles on the map
