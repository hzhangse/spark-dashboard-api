package utils

import (
	"context"
	"log"
	"os"
	promremote "spark-api/prom"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
var (
	brokers                  = ""
	version                  = "3.2.0"
	group                    = ""
	topics                   = ""
	assignor                 = ""
	oldest                   = true
	verbose                  = false
	batchSendNum       int64 = 500
	admin              sarama.ClusterAdmin
	kafkaClient        sarama.Client
	minBlockDuration                        = "10m"
	consumerGroupCache map[string]*Consumer = make(map[string]*Consumer)

	topicCache         sync.Map
	finishedTopicCache sync.Map

	//producer sarama.SyncProducer
)

func init() {
	//Kafka consumer group definition
	brokers = os.Getenv("KafkaBrokers")
	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}
	//Kafka consumer group definition
	group = os.Getenv("ConsumeGroup")
	if len(group) == 0 {
		group = "sparkmeasure-cg"
	}
	//Kafka topics to be consumed, as a comma seperated list
	topics = os.Getenv("KafkaTopic")
	if len(topics) == 0 {
		topics = "sparkmeasure-stageinfo"
	}

	//Kafka topics to be consumed, as a comma seperated list
	batchSendNumStr := os.Getenv("BatchSendNum")
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

	minBlockDuration = os.Getenv("minBlockDuration")

	initKafka()
	go CreateConsumer(topics, true)
}

func initKafka() {
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)

	}
	config.Version = version
	config.Producer.RequiredAcks = sarama.WaitForAll // -1

	config.Producer.Return.Successes = true

	if kafkaClient != nil && !kafkaClient.Closed() {
		kafkaClient.Close()
		admin.Close()
	}

	kafkaClient, err = sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil {
		log.Panicf("Error new kafka client: %v", err)

	}

	admin, err = sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		log.Panicf("Error new cluster admin: %v", err)
	}

	//producer, err := sarama.NewSyncProducer([]string{brokers}, config)
	//producer, err = sarama.NewSyncProducerFromClient(kafkaClient)
	// defer func() {
	// 	_ = producer.Close()
	// }()

	// alltopics, err := admin.ListTopics()
	// sparkAppTopics := make([]string, 0)
	// if err == nil {
	// 	for k, _ := range alltopics {
	// 		if strings.HasPrefix(k, topics) {
	// 			if k != topics {
	// 				sparkAppTopics = append(sparkAppTopics, k)
	// 				//cleanFinishedTopic(k)
	// 			}
	// 		}
	// 	}

	// if len(sparkAppTopics) > 0 {
	// 	topicMetadatas, err := admin.DescribeTopics(sparkAppTopics)
	// 	if err == nil {
	// 		for _, metadata := range topicMetadatas {
	// 			topicName := metadata.Name
	// 			topicPartition := make([]int32, 0)
	// 			for _, partition := range metadata.Partitions {
	// 				topicPartition = append(topicPartition, (*partition).ID)
	// 			}

	// 			succ := checkTopicPartitionConsumedSuccess(topicName, topicPartition)
	// 			if !succ {
	// 				go CreateConsumer(topicName, false)
	// 			} else {
	// 				go cleanFinishedTopic(topicName)
	// 			}

	// 		}
	// 	}
	// }
	//}
}

func checkTopicConsumedFinished(sparkAppTopic string) bool {
	succ := false
	i := 0
	for {
		i++
		succ, err = _checkTopicConsumedFinished(sparkAppTopic)
		if err != nil && i <= 3 {
			//try reconnect kafka
			initKafka()
		} else {
			log.Printf("Topic: %s, ConsumedFinished: %t", sparkAppTopic, succ)
			break
		}
	}
	return succ
}

func _checkTopicConsumedFinished(sparkAppTopic string) (bool, error) {
	succ := false
	sparkAppTopics := strings.Split(sparkAppTopic, ",")

	if len(sparkAppTopics) > 0 {

		topicMetadatas, err := admin.DescribeTopics(sparkAppTopics)
		if err == nil {
			for _, metadata := range topicMetadatas {
				topicName := metadata.Name
				topicPartition := make([]int32, 0)
				for _, partition := range metadata.Partitions {
					topicPartition = append(topicPartition, (*partition).ID)
				}

				succ, err = checkTopicPartitionConsumedSuccess(topicName, topicPartition)
				if err != nil {
					return false, err
				}

			}
		} else {
			return false, err
		}
	}
	return succ, nil
}

func checkTopicPartitionConsumedSuccess(topic string, partiion []int32) (bool, error) {
	tm := make(map[string][]int32)
	tm[topic] = partiion
	res, err := admin.ListConsumerGroupOffsets(topic, tm)
	if err != nil {
		return false, err
	}
	succ := true
	for _topic, _consumerPartions := range (*res).Blocks {
		for index, _consumerPartion := range _consumerPartions {
			_topicParOffset, err := kafkaClient.GetOffset(_topic, index, sarama.OffsetNewest)
			if err != nil {
				return false, err
			}

			// log.Println("offset for:" + topic + " partion:" + GetInterfaceToString(index) +
			// 	" topic-offset:" + GetInterfaceToString(_topicParOffset) + " consumer-offset:" + GetInterfaceToString(_consumerPartion.Offset))
			if _topicParOffset > 0 && _topicParOffset != _consumerPartion.Offset {
				succ = false
			}
		}
	}
	return succ, nil
}

func cleanFinishedTopic(topicName string) {
	err = admin.DeleteTopic(topicName)
	if err == nil {
		log.Printf("clean topic: %s", topicName)
	} else {
		log.Printf("clean topic %s failed, %v ", topicName, err)
	}
	// err = admin.DeleteConsumerGroup(topicName)

	// if err == nil {
	// 	log.Printf("clean consumeGroup: %s", topicName)
	// } else {
	// 	log.Printf("clean consumeGroup %s failed, %v  ", topicName, err)
	// }
}

func CreateProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // -1

	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{brokers}, config)
	defer func() {
		_ = producer.Close()
	}()

	return producer, err

}

func CreateConsumer(topic string, p2pMode bool) error {

	cgStr := topic
	if p2pMode {
		hostIp, err := GetExternalIP()
		if err == nil {
			cgStr = topic + "-" + hostIp.String()
		}
	}

	_, ok := consumerGroupCache[cgStr]
	if ok {
		return nil
	}
	log.Printf("Starting a new Sarama consumer(%s)...", cgStr)
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
		return err
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
		ready: make(chan bool, 1),
		group: cgStr,
	}

	ctx, cancel := context.WithCancel(context.Background())

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), cgStr, config)
	//consumerGroup, err := sarama.NewConsumerGroupFromClient(cgStr, kafkaClient)
	if err != nil {
		log.Printf("Error creating consumer group(%s) client: %v", cgStr, err)
		cancel()
		err = consumerGroup.Close()
		if err != nil {
			log.Printf("Error closing  consumer(%s): %v", cgStr, err)
		}
		return err
	}

	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumerGroup.Consume(ctx, strings.Split(topic, ","), &consumer); err != nil {
				log.Printf("Error from consumer(%s): %v", cgStr, err)

			}
			// check if context was cancelled, signaling that the consumer should stop
			if err = ctx.Err(); err != nil {
				log.Printf("context of consumer(%s) was cancelled: %v", cgStr, err)
				return
			}

			consumer.ready = make(chan bool, 1)
			consumer.group = cgStr
		}
	}()

	isUp := <-consumer.ready // Await till the consumer has been set up
	if isUp {
		consumerGroupCache[cgStr] = &consumer

	}

	isUp = <-consumer.ready
	if !isUp {
		cancel()
		err = consumerGroup.Close()
		if err != nil {
			log.Printf("Error closing  consumer(%s): %v", cgStr, err)
		}
	}

	return nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	group string
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sess sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	consumer.ready <- true
	//close(consumer.ready)
	log.Printf("Sarama consumer(%s) Setup!", consumer.group)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	//close(consumer.ready)
	log.Printf("Sarama consumer(%s) Cleanup!", consumer.group)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	claimTopic := claim.Topic()
	if claimTopic == topics {
		return consumer.ConsumeTopicMsg(session, claim)
	} else {
		return consumer.ConsumeAppMsg(session, claim)
	}

}

func (consumer *Consumer) ConsumeTopicMsg(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
				ch, chexist := msgMap[currentKey]
				_, exist := readyMap[currentKey]
				// no correspond channel in cache, create new and put to cache
				if !exist && !chexist {
					dealAppTopicChannel(&readyMap, &msgMap, session)
					ch = make(chan *sarama.ConsumerMessage, batchSendNum)
					readyChan := make(chan bool)
					msgMap[currentKey] = ch
					readyMap[currentKey] = readyChan

				}
				ch <- currentMessage
				if len(ch) == cap(ch) {
					dealAppTopicChannel(&readyMap, &msgMap, session)
				}
				//log.Printf("topic message: %s", string(currentMessage.Value))
			}

		case <-time.After(time.Millisecond * 200):
			dealAppTopicChannel(&readyMap, &msgMap, session)
		case <-session.Context().Done():
			return nil

		}
	}
}

func dealAppTopicChannel(readyMap *map[string]chan bool, msgMap *map[string]chan *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	for currentKey, readyChan := range *readyMap {
		go func() {
			readyChan <- true
			close(readyChan)
		}()
		ch := (*msgMap)[currentKey]
		close(ch)
		readSendDealTopic(ch, readyChan, session, currentKey)
		delete(*msgMap, currentKey)
		delete(*readyMap, currentKey)

	}
}

func readSendDealTopic(ch <-chan *sarama.ConsumerMessage, ready chan bool, session sarama.ConsumerGroupSession, currentKey string) {
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

	i := 0
	topic := topics + "-" + currentKey
	for message := range ch {
		currentMessage = message
		if i == 0 {
			beginMessage = message
		}

		currentMessageValue := string(currentMessage.Value)
		_map, err := convertMap(currentMessageValue)
		if err == nil {
			name := GetInterfaceToString(_map["name"])
			_, currentKey, err := convertTimeSeries(_map)

			if err == nil && currentKey != "" && currentKey != "noAppId" {
				if name == "jobs_started" {
					//if _, ok := finishedTopicCache.Load(topic); !ok {
					topicCache.Store(topic, name)
					//}

				} else if name == "jobs_ended" {
					//topicCache.Delete(topic)
					finishedTopicCache.Store(topic, name)

				}
			}
		}
		i++
	}
	succ := checkTopicConsumedFinished(topic)
	_, ok := topicCache.Load(topic)
	if ok {
		go func(topic string) {

			_, readyExist := consumerGroupCache[topic]
			if !succ && !readyExist {
				session.MarkMessage(currentMessage, "")
				CreateConsumer(topic, false)
			}

		}(topic)
		topicCache.Delete(topic)
	}

	_, ok = finishedTopicCache.Load(topic)
	if ok {
		ticker := time.NewTicker(15 * time.Second)

		go func(topic string) {
			for {
				comsumer, readyExist := consumerGroupCache[topic]
				if succ {
					session.MarkMessage(currentMessage, "")
					ticker.Stop()
					cleanFinishedTopic(topic)

					if readyExist {
						//*ready <- false
						close((*comsumer).ready)
						delete(consumerGroupCache, topic)
					}
					break
				}
				<-ticker.C
				log.Printf("Checking topic(%s) consuming status", topic)
				succ = checkTopicConsumedFinished(topic)
			}

		}(topic)
		finishedTopicCache.Delete(topic)
	}

	if err == nil {
		//session.MarkMessage(currentMessage, "")
		log.Printf("Message consumed: topic=%s, hash-key=%s, partition=%d, from-to offset{%d--%d}, message: %s", (*currentMessage).Topic, currentKey, (*currentMessage).Partition, (*beginMessage).Offset, (*currentMessage).Offset, (*currentMessage).Value)

	}

}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeAppMsg(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var claimMessageChan = claim.Messages()
	var currentMessage *sarama.ConsumerMessage
	msgMap := make(map[string]chan *promremote.TimeSeries)
	readyMap := make(map[string]chan bool)
	for {
		select {
		case message, ok := <-claimMessageChan:
			if ok {
				currentMessage = message
				currentMessageValue := string(currentMessage.Value)
				_map, err := convertMap(currentMessageValue)
				if err == nil {
					ts, currentKey, err := convertTimeSeries(_map)
					if err == nil && currentKey != "" {
						ch, chexist := msgMap[currentKey]
						_, exist := readyMap[currentKey]
						// no correspond channel in cache, create new and put to cache
						if !exist && !chexist {
							ch = make(chan *promremote.TimeSeries, batchSendNum)
							readyChan := make(chan bool)
							msgMap[currentKey] = ch
							readyMap[currentKey] = readyChan

						}
						ch <- &ts
						if len(ch) == cap(ch) {
							dealReadyChannel(&readyMap, &msgMap, currentMessage, session)
						}
					}
					//deal with cg when jobs_ended triggered
					// name := _map["name"]
					// if name == "jobs_ended" {
					// 	log.Printf("jobs_ended message: %s", currentMessageValue)

					// 	msg := &sarama.ProducerMessage{
					// 		Topic: topics,
					// 		Key:   sarama.StringEncoder(currentKey),
					// 		Value: sarama.StringEncoder(currentMessageValue),
					// 	}
					// 	_, _, err = producer.SendMessage(msg)
					// 	if err != nil {
					// 		log.Printf("produce jobs_ended message failed : %v", err)
					// 	}
					// }
				}

			}

		case <-time.After(time.Millisecond * 200):
			dealReadyChannel(&readyMap, &msgMap, currentMessage, session)
		case <-session.Context().Done():
			return nil

		}
	}

}

func dealReadyChannel(readyMap *map[string]chan bool, msgMap *map[string]chan *promremote.TimeSeries, currentMessage *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	for currentKey, readyChan := range *readyMap {
		go func() {
			readyChan <- true
			close(readyChan)
		}()
		ch := (*msgMap)[currentKey]
		close(ch)
		readyToSend(ch, readyChan, session, currentMessage, currentKey)
		delete(*msgMap, currentKey)
		delete(*readyMap, currentKey)

	}
}

func readyToSend(ch <-chan *promremote.TimeSeries, ready chan bool, session sarama.ConsumerGroupSession, currentMessage *sarama.ConsumerMessage, currentKey string) {
re:
	for {
		select {
		case canRead := <-ready:
			if canRead {
				break re
			}

		}
	}

	tslist := make([]promremote.TimeSeries, 0)
	var i int64 = 0
	for ts := range ch {
		tslist = append(tslist, *ts)
		i++
	}

	markMessageIfSendPromSuccess(tslist, currentMessage, currentKey, session)

}

func markMessageIfSendPromSuccess(tslist []promremote.TimeSeries, currentMessage *sarama.ConsumerMessage, currentKey string, session sarama.ConsumerGroupSession) error {
	//err := send2Prom(tslist, currentKey)

	//if err1 == nil {
	session.MarkMessage(currentMessage, "")
	beginoffset := (*currentMessage).Offset - int64(len(tslist)) + 1
	log.Printf("Message consumed: topic=%s,  partition=%d, from-to offset{%d--%d}", (*currentMessage).Topic, (*currentMessage).Partition, beginoffset, (*currentMessage).Offset)
	//}
	return nil
}

// func readAndSend(ch <-chan *sarama.ConsumerMessage, ready chan bool, session sarama.ConsumerGroupSession, currentKey string) {
// 	re:
// 		for {
// 			select {
// 			case canRead := <-ready:
// 				if canRead {
// 					break re
// 				}

// 			}
// 		}

// 		var currentMessage *sarama.ConsumerMessage
// 		var beginMessage *sarama.ConsumerMessage
// 		msgBatch := make([]string, 0, batchSendNum)
// 		i := 0
// 		for message := range ch {
// 			currentMessage = message
// 			if i == 0 {
// 				beginMessage = message
// 			}

// 			msg := string(currentMessage.Value)
// 			msgBatch = append(msgBatch, msg)
// 			i++
// 		}

// 		err := WriteInBatch(msgBatch, currentKey)
// 		if err == nil {
// 			session.MarkMessage(currentMessage, "")
// 			log.Printf("Message consumed: topic=%s, hash-key=%s, partition=%d, from-to offset{%d--%d}", (*currentMessage).Topic, currentKey, (*currentMessage).Partition, (*beginMessage).Offset, (*currentMessage).Offset)
// 			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partition = %s, offset= %s", (*currentMessage).Value, (*currentMessage).Timestamp, (*currentMessage).Topic, (*currentMessage).Partition, (*currentMessage).Offset)
// 		}

// 	}
// func dealExistChannel(readyMap *map[string]chan bool, msgMap *map[string]chan *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
// 	for currentKey, readyChan := range *readyMap {
// 		go func() {
// 			readyChan <- true
// 			close(readyChan)
// 		}()
// 		ch := (*msgMap)[currentKey]
// 		close(ch)
// 		readAndSend(ch, readyChan, session, currentKey)
// 		delete(*msgMap, currentKey)
// 		delete(*readyMap, currentKey)

// 	}
// }
// func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
// 	var claimMessageChan = claim.Messages()
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
// 					dealExistChannel(&readyMap, &msgMap, session)
// 					ch = make(chan *sarama.ConsumerMessage, batchSendNum)
// 					readyChan = make(chan bool)
// 					msgMap[currentKey] = ch
// 					readyMap[currentKey] = readyChan

// 				}
// 				ch <- currentMessage
// 				if len(ch) == cap(ch) {
// 					dealExistChannel(&readyMap, &msgMap, session)
// 				}

// 			}

// 		case <-time.After(time.Millisecond * 100):
// 			dealExistChannel(&readyMap, &msgMap, session)
// 		case <-session.Context().Done():
// 			return nil

// 		}
// 	}

// 	//	return dispatchMessage(claimMessageChan, session)

// }

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
