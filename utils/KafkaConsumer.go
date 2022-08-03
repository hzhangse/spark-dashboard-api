package utils

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
var (
	brokers            = ""
	version            = "3.2.0"
	group              = ""
	topics             = ""
	assignor           = ""
	oldest             = true
	verbose            = false
	batchSendNum int64 = 100
)

func init() {
	//Kafka consumer group definition
	brokers = os.Getenv("Brokers")
	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}
	//Kafka consumer group definition
	group = os.Getenv("consume-group")
	if len(group) == 0 {
		group = "sparkmeasure-cg"
	}
	//Kafka topics to be consumed, as a comma seperated list
	topics = os.Getenv("Topic")
	if len(topics) == 0 {
		topics = "sparkmeasure-stageinfo"
	}

	//Kafka topics to be consumed, as a comma seperated list
	batchSendNumStr := os.Getenv("batchSendNum")
	if len(batchSendNumStr) > 0 {
		batchSendNum, _ = strconv.ParseInt(batchSendNumStr, 10, 64)
	}
	//Consumer group partition assignment strategy (range, roundrobin, sticky)
	assignor = os.Getenv("Assignor")
	if len(assignor) == 0 {
		assignor = "range"
	}
	//Kafka consumer consume initial offset from oldest
	oldestStr := os.Getenv("Oldest")
	if len(oldestStr) > 0 {
		oldest, _ = strconv.ParseBool(oldestStr)
	}

	//logging
	verboseStr := os.Getenv("Verbose")
	if len(verboseStr) > 0 {
		verbose, _ = strconv.ParseBool(verboseStr)
	}

}

func Consume() {

	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, _ := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	go func() {
		defer func() {

			if err = client.Close(); err != nil {
				log.Panicf("Error closing client: %v", err)
			}
		}()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

}

func consume() {

	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var claimMessageChan = claim.Messages()
	var currentMessage *sarama.ConsumerMessage
	msgMap := make(map[string]chan *sarama.ConsumerMessage)
	readyMap := make(map[string]chan bool)
	for {
		select {
		case message, ok := <-claimMessageChan:
			if ok {
				currentMessage = message
				currentKey := string(currentMessage.Key)
				ch, exist := msgMap[currentKey]
				readyChan, exist := readyMap[currentKey]
				// no correspond channel in cache, create new and put to cache
				if !exist {
					dealExistChannel(&readyMap, &msgMap, session)
					ch = make(chan *sarama.ConsumerMessage, batchSendNum)
					readyChan = make(chan bool)
					msgMap[currentKey] = ch
					readyMap[currentKey] = readyChan

				}
				ch <- currentMessage
				if len(ch) == cap(ch) {
					dealExistChannel(&readyMap, &msgMap, session)
				}

			}

		case <-time.After(time.Second * 1):
			dealExistChannel(&readyMap, &msgMap, session)
		case <-session.Context().Done():
			return nil

		}
	}

	//	return dispatchMessage(claimMessageChan, session)

}

func dealExistChannel(readyMap *map[string]chan bool, msgMap *map[string]chan *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	for currentKey, readyChan := range *readyMap {
		go func() {
			readyChan <- true
			close(readyChan)
		}()
		ch := (*msgMap)[currentKey]
		close(ch)
		readAndSend(ch, readyChan, session, currentKey)
		delete(*msgMap, currentKey)
		delete(*readyMap, currentKey)

	}
}
func readAndSend(ch <-chan *sarama.ConsumerMessage, ready chan bool, session sarama.ConsumerGroupSession, currentKey string) {
re:
	for {
		select {
		case canRead := <-ready:
			if canRead {
				break re
			}

		}
	}

	var currentMessage *sarama.ConsumerMessage
	var beginMessage *sarama.ConsumerMessage
	msgBatch := make([]string, 0, batchSendNum)
	i := 0
	for message := range ch {
		currentMessage = message
		if i == 0 {
			beginMessage = message
		}

		msg := string(currentMessage.Value)
		msgBatch = append(msgBatch, msg)
		i++
	}

	err := WriteInBatch(msgBatch, currentKey)
	if err == nil {
		session.MarkMessage(currentMessage, "")
		log.Printf("Message consumed: topic=%s, hash-key=%s, partition=%d, from-to offset{%d--%d}", (*currentMessage).Topic, currentKey, (*currentMessage).Partition, (*beginMessage).Offset, (*currentMessage).Offset)
		//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partition = %s, offset= %s", (*currentMessage).Value, (*currentMessage).Timestamp, (*currentMessage).Topic, (*currentMessage).Partition, (*currentMessage).Offset)
	}

}

// func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
// 	var claimMessageChan = claim.Messages()

// 	msgMap := make(map[string][]string)
// 	index := 0
// 	var currentMessage *sarama.ConsumerMessage
// 	for {
// 		select {
// 		case message, ok := <-claimMessageChan:
// 			if ok {
// 				currentMessage = message
// 				currentKey := string(currentMessage.Key)
// 				msgBatch, ok := msgMap[currentKey]
// 				if !ok {
// 					// 不存在appid
// 					msgBatch = make([]string, 0, batchSendNum)
// 					msgMap[currentKey] = msgBatch
// 				}
// 				msg := string(currentMessage.Value)

// 				msgBatch = append(msgBatch, msg)
// 				index++
// 				if index >= int(batchSendNum) {
// 					batchRemoteWrite(session, &currentMessage, &msgBatch, &index)
// 				}

// 			}

// 		case <-time.After(time.Second * 1):
// 			if currentMessage != nil {
// 				currentKey := string(currentMessage.Key)
// 				msgBatch, ok := msgMap[currentKey]
// 				if ok {
// 					batchRemoteWrite(session, &currentMessage, &msgBatch, &index)
// 				}

// 			}
// 		case <-session.Context().Done():
// 			return nil
// 		}
// 	}

// }

// func dispatchMessage(claimMessageChan <-chan *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
// 	var currentMessage *sarama.ConsumerMessage
// 	msgMap := make(map[string]chan *sarama.ConsumerMessage)
// 	readyMap := make(map[string]chan bool)
// 	for {
// 		select {
// 		case message, ok := <-claimMessageChan:
// 			if ok {
// 				currentMessage = message
// 				currentKey := string(currentMessage.Key)
// 				ch, exist := msgMap[currentKey]
// 				readyChan, exist := readyMap[currentKey]
// 				// no correspond channel in cache, create new and put to cache
// 				if !exist {
// 					ch = make(chan *sarama.ConsumerMessage, batchSendNum)
// 					readyChan := make(chan bool)
// 					msgMap[currentKey] = ch
// 					readyMap[currentKey] = readyChan
// 				}
// 				ch <- currentMessage
// 				if len(ch) == cap(ch) {
// 					readyChan <- true
// 					close(ch)
// 					close(readyChan)
// 					delete(msgMap, currentKey)
// 					delete(readyMap, currentKey)
// 				}
// 				go readAndSend(ch, readyChan, session, currentKey)
// 			}

// 		case <-time.After(time.Second * 1):
// 			for _, v := range readyMap {
// 				v <- true
// 			}
// 		case <-session.Context().Done():
// 			return nil

// 		}
// 	}

// }

// func batchRemoteWrite(session sarama.ConsumerGroupSession, currentMessage **sarama.ConsumerMessage, msgBatch *[]string, index *int) error {

// 	err := WriteInBatch(*msgBatch)
// 	if err == nil {
// 		session.MarkMessage(*currentMessage, "")
// 		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partition = %s, offset= %s", (*currentMessage).Value, (*currentMessage).Timestamp, (*currentMessage).Topic, (*currentMessage).Partition, (*currentMessage).Offset)
// 	}
// 	*index = 0
// 	*msgBatch = (*msgBatch)[0:0]
// 	*currentMessage = nil
// 	return err
// }
