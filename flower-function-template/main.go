package main

import (
	"context"
	"log"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
)

// Define a struct representing the data for our expected event.
// "payload" field will consist of all previous functions results data in the next way:
//"payload": {
//	"first-func": {
//		"some-data": "...."
//	},
//	"second-func": {
//		"more-data": "..."
//	}
//}
type eventBody struct {
	Sequence int                    `json:"id"`
	Payload  map[string]interface{} `json:"payload"`
}

func gotEvent(inputEvent event.Event) (*event.Event, protocol.Result) {
	//Unmarshal event body in our struct
	data := &eventBody{}
	if err := inputEvent.DataAs(data); err != nil {
		log.Printf("Got error while unmarshalling data: %s", err.Error())
		return nil, http.NewResult(400, "got error while unmarshalling data: %w", err)
	}

	log.Println("Received a new event: ")
	log.Printf("[%v] %s %s: %+v", inputEvent.Time(), inputEvent.Source(), inputEvent.Type(), data)

	// Create output event
	outputEvent := inputEvent.Clone()

	/*
		--
		--
		YOUR BUSINESS LOGIC HERE...
		you can use data from previous functions by addressing it like that:
		someData := data.Payload["previous-func-name"]
		or to access initial data
		initData := data.Payload["body"]
		--
		--
	*/

	// as a result of your business logic execution, you will have some data which you need to transmit to the next function, add it to payload with the new key - current-func-name
	data.Payload["current-func-name"] = "{your data json representation here}"

	// Set new payload to the output event
	if err := outputEvent.SetData(cloudevents.ApplicationJSON, data); err != nil {
		log.Printf("Got error while marshalling data: %s", err.Error())
		return nil, http.NewResult(500, "got error while marshalling data: %w", err)
	}

	log.Println("Transform the event to: ")
	log.Printf("[%s] %s %s: %+v", outputEvent.Time(), outputEvent.Source(), outputEvent.Type(), data)

	// Return event to the client, event will be sent to the next node by client
	return &outputEvent, nil
}

func main() {
	// Init cloudEvents client
	c, err := cloudevents.NewDefaultClient()
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Println("listening on 8080")
	// Start listening for new events...
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(context.Background(), gotEvent))
}
