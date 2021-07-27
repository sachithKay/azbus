package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-service-bus-go"
	"os"
	"time"
)

type (
	Scientist struct {
		Surname   string `json:"surname,omitempty"`
		FirstName string `json:"firstname,omitempty"`
	}
)

type PriorityPrinter struct {
	SubName string
}

func main() {

	// connection information
	topicName := "temp_topic_sachith"
	subscriptionName := "sub1"
	connString := "Endpoint=sb://choreohub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fKRGgXpK7HNyVrF8yqDbPoApvsGBekaFzCydzuAlS78="

	// Listen for 1 minute only
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connString))

	topic, err := getTopic(ctx, ns, topicName, subscriptionName)
	if err != nil {
		fmt.Printf("failed to build a new topic named %q\n", topicName)
		os.Exit(1)
	}

	go sendMessages(ctx, topic)

	receiveMessages(topic, subscriptionName)

}

func receiveMessages(topic *servicebus.Topic, subscriptionName string)  {

	ctx := context.Background()
	subscription, err := topic.NewSubscription(subscriptionName)
	if subscription == nil {
		fmt.Println("NewSubscription is null")
		os.Exit(1)
	}


	var receiverOpts []servicebus.ReceiverOption
	subReceiver, err := subscription.NewReceiver(ctx, receiverOpts...)
	if err != nil {
		fmt.Println(err)
	}

	listenerHandle := subReceiver.Listen(ctx, servicebus.HandlerFunc(func(ctx context.Context, message *servicebus.Message) error {
		fmt.Println(string(message.Data) + ": received")
		return message.Complete(ctx)
	}))

	listenerHandle2 := subReceiver.Listen(ctx, servicebus.HandlerFunc(func(ctx context.Context, message *servicebus.Message) error {
		fmt.Println(string(message.Data) + ": received2")
		return message.Complete(ctx)
	}))


	<-listenerHandle.Done()
	if err := subReceiver.Close(ctx); err != nil {
		fmt.Println("subReceiver.Closer", err)
	}

	<-listenerHandle2.Done()
	if err := subReceiver.Close(ctx); err != nil {
		fmt.Println("subReceiver.Closer", err)
	}
}


func sendMessages(ctx context.Context, t *servicebus.Topic) {

	scientists := []Scientist{
		{
			Surname:   "Einstein",
			FirstName: "Albert",
		},
		{
			Surname:   "Heisenberg",
			FirstName: "Werner",
		},
		{
			Surname:   "Sachith",
			FirstName: "K",
		},
		{
			Surname:   "Adam",
			FirstName: "Mann",
		},
		{
			Surname:   "Hell",
			FirstName: "Bell",
		},
		{
			Surname:   "Dude",
			FirstName: "There",
		},
		{
			Surname:   "Gone",
			FirstName: "Mad",
		},
		{
			Surname:   "Dam",
			FirstName: "Stuff",
		},
	}

	for _, scientist := range scientists {
		bits, err := json.Marshal(scientist)
		if err != nil {
			fmt.Println(err)
			fmt.Errorf("marshal")
			return
		}

		ttl := 2 * time.Minute
		msg := servicebus.NewMessage(bits)
		msg.ContentType = "application/json"
		msg.TTL = &ttl

		if err := t.Send(ctx, msg); err != nil {
			fmt.Errorf("send")
			fmt.Println(err)
			return
		}
	}
}

func getTopic(ctx context.Context, ns *servicebus.Namespace, topicName string, subscriptionName string) (*servicebus.Topic, error) {

	subManagerStruct, err := ns.NewSubscriptionManager(topicName)

	if err != nil {
		fmt.Println("!!!! Error occurred in getTopic")
		fmt.Println(err)
	}

	fmt.Println("executing subManagerStruct.Get")
	subscriptionEntity, err := subManagerStruct.Get(ctx, subscriptionName)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Done executing subManagerStruct.Get")

	if subscriptionEntity == nil {
		fmt.Println("!Subscription does not exist. Trying to create")
		_, err = subManagerStruct.Put(ctx, subscriptionName)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Subscription created")
		}

	}

	return subManagerStruct.Topic, err
}
