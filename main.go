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
	Organization struct {
		Handle   string `json:"handle,omitempty"`
	}
)


func main() {

	// connection information
	topicName := "temp_topic_sachith"
	subscriptionName := "sub1"
	connString := "Endpoint=sb://choreohub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=fKRGgXpK7HNyVrF8yqDbPoApvsGBekaFzCydzuAlS78="

	// Listen for 1 minute only
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ns, _ := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connString))

	topic, terr := getTopic(ctx, ns, topicName, subscriptionName)
	if terr != nil {
		fmt.Printf("failed to build a new topic named %q\n", topicName)
		for i := 1; i <= 10; i++ {
			fmt.Printf("Attempting retrying %v\n", i)
			topic, terr = getTopic(ctx, ns, topicName, subscriptionName)
			if terr == nil {
				break
			}

			if i == 10 {
				fmt.Println("Maximum retry attempts reached. Application will stop the execution. Goodnight!")
				return
			}

			// wait 5 seconds before the next attempt
			time.Sleep(5 * time.Second)
		}
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
		//time.Sleep(30 * time.Second)
		fmt.Println(string(message.Data) + ": received")
		return message.Complete(ctx)
	}))


	<-listenerHandle.Done()
	if err := subReceiver.Close(ctx); err != nil {
		fmt.Println("subReceiver.Closer", err)
	}

	fmt.Println("Retrying to establish connection")
	receiveMessages(topic, subscriptionName)
}


func sendMessages(ctx context.Context, t *servicebus.Topic) {

	organizations := []Organization{
		{
			Handle:   "a",
		},
		{
			Handle:   "s",
		},
		{
			Handle:   "d",
		},
		{
			Handle:   "f",
		},
		{
			Handle:   "g",
		},
		{
			Handle:   "h",
		},
		{
			Handle:   "j",
		},
	}

	for _, organization := range organizations {
		bits, err := json.Marshal(organization)
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
		fmt.Printf("----> %v\n", err)
		return nil, err
	}

	fmt.Println("executing subManagerStruct.Get")
	subscriptionEntity, err := subManagerStruct.Get(ctx, subscriptionName)
	if err != nil {
		fmt.Printf("++++ %v\n", err)
		return nil, err
	}

	if subscriptionEntity == nil {
		fmt.Println("Subscription does not exist. Trying to create")
		_, err = subManagerStruct.Put(ctx, subscriptionName)
		if err != nil {
			fmt.Printf(".................. %v\n", err)
			return nil, err
		} else {
			fmt.Println("Subscription created")
		}
	}

	return subManagerStruct.Topic, err
}
