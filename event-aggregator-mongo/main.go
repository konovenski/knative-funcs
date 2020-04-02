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
	"log"
	"os"
	"strconv"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	"github.com/kelseyhightower/envconfig"

	"gopkg.in/mgo.v2"
)

var now = time.Now()
var session, _ = mgo.Dial(os.Getenv("MONGO_HOST"))
var c = session.DB("sink").C("event" + now.Format("20060102150405"))

type envConfig struct {
	Msg  string `envconfig:"MESSAGE" default:"boring default msg, change me with env[MESSAGE]"`
	Type string `envconfig:"TYPE"`
}

var (
	env envConfig
)

const volumePath string = "/etc/config"

// Define a small struct representing the data for our expected data.
type cloudEventBaseData struct {
	Message  string `json:"message"`
	Sequence int    `json:"id"`
}

func gotEvent(event cloudevents.Event, resp *cloudevents.EventResponse) error {
	log.Println("Trying to receive events...")
	ctx := event.Context.AsV1()
	data := &cloudEventBaseData{}
	if err := event.DataAs(data); err != nil {
		log.Printf("Got Data Error: %s\n", err.Error())
		return err
	}

	log.Println("Received a new event: ")
	log.Printf("[%s] %s %s: %+v", ctx.Time.String(), ctx.GetSource(), ctx.GetType(), data)

	log.Printf("Expect %v funcs", os.Getenv("NUMBER_OF_FUNCS"))
	nbr, _ := strconv.Atoi(os.Getenv("NUMBER_OF_FUNCS"))
	sinked := sinkEvents(data, nbr)
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
	cl, err := cloudevents.NewDefaultClient()
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	session.SetMode(mgo.Primary, true)
	defer session.Close()
	log.Printf("Current mongo collection is %s", c.Name)

	log.Printf("listening on 8080, appending %q to events", env.Msg)
	log.Fatalf("failed to start receiver: %s", cl.StartReceiver(context.Background(), gotEvent))
}

func sinkEvents(cevents *cloudEventBaseData, numberOfFuncs int) bool {
	events := []cloudEventBaseData{}
	log.Println("Insert event into mongo collection....")
	_ = c.Insert(cevents)
	log.Println("Fetch all events from mongo collection....")
	_ = c.Find(nil).All(&events)
	log.Printf("Found %d events", len(events))
	log.Printf("Expect %d events", numberOfFuncs)
	if len(events) == numberOfFuncs {
		message := ""
		for _, event := range events {
			message += event.Message
		}
		cevents.Message = message
		log.Printf("Event sink complete with message %s", message)
		return true
	}
	return false
}
