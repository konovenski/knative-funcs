/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/kelseyhightower/envconfig"
)

type envConfig struct {
	Msg           string `envconfig:"MESSAGE" default:"boring default msg, change me with env[MESSAGE]"`
	Type          string `envconfig:"TYPE"`
	NumberOfFuncs int    `envconfig:"NumberOfFuncs"`
}

var (
	env envConfig
)

const volumePath string = "/etc/config"

// Define a small struct representing the data for our expected data.
type cloudEventBaseData struct {
	Sequence int    `json:"id"`
	Message  string `json:"message"`
}

func gotEvent(event cloudevents.Event, resp *cloudevents.EventResponse) error {
	ctx := event.Context.AsV1()

	data := &cloudEventBaseData{}
	if err := event.DataAs(data); err != nil {
		log.Printf("Got Data Error: %s\n", err.Error())
		return err
	}

	log.Println("Received a new event: ")
	log.Printf("[%s] %s %s: %+v", ctx.Time.String(), ctx.GetSource(), ctx.GetType(), data)

	// append eventMsgAppender to message of the data
	sinked := sinkEvents(data, env.NumberOfFuncs)
	r := cloudevents.Event{
		Context: ctx,
		Data:    data,
	}

	// Resolve type
	if env.Type != "" {
		r.Context.SetType(env.Type)
	}

	r.SetDataContentType(cloudevents.ApplicationJSON)
	if sinked {
		resp.RespondWith(200, &r)
	}
	return nil
}

func main() {
	log.Println("Start initializing func..")
	if err := envconfig.Process("", &env); err != nil {
		log.Printf("[ERROR] Failed to process env var: %s", err)
		os.Exit(1)
	}
	log.Println("Getting Cloud Event client..")
	c, err := cloudevents.NewDefaultClient()

	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Printf("listening on 8080, appending %q to events", env.Msg)
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(context.Background(), gotEvent))
}

func sinkEvents(cevents *cloudEventBaseData, numberOfFuncs int) bool {
	filePath := filepath.Join(volumePath, "event.json")

	cevents.Sequence = 1
	if fileExists(filePath) {
		log.Println("Reading previous events....")
		prevEvent := &cloudEventBaseData{}
		file, _ := ioutil.ReadFile(filePath)
		err := json.Unmarshal(file, prevEvent)
		if err != nil {
			log.Fatalf("Unable to read event sinking file, %v", err)
		} else {
			cevents.Message = prevEvent.Message + " " + cevents.Message
			cevents.Sequence = prevEvent.Sequence + 1
		}
	}
	if cevents.Sequence == numberOfFuncs {
		log.Println("Event sink complete, remove file....")
		os.Remove(filePath)
		return true
	}
	log.Println("Write to sink file....")
	file, err := os.OpenFile(filePath, os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Fatalf("Unable to create/open event sinking file, %v", err)
		return false
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	err = encoder.Encode(cevents)
	if err != nil {
		log.Fatalf("Unable to write into event sinking file, %v", err)
	}
	return false
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
