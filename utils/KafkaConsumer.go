package utils

import (
	"container/list"
	"context"
	"log"
	"os"
	promremote "spark-api/prom"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/avast/retry-go/v4"
)

// Sarama configuration options
var (
	brokers                     = ""
	version                     = "3.2.0"
	group                       = ""
	topics                      = ""
	assignor                    = ""
	oldest                      = true
	verbose                     = false
	batchSendNum          int64 = 500
	admin                 sarama.ClusterAdmin
	kafkaClient           sarama.Client
	minBlockDuration                           = "1H"
	minBlockDurationValue                      = 1
	minBlockDurationUnit                       = "H"
	consumerGroupCache    map[string]*Consumer = make(map[string]*Consumer)

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

	minBlockDuration = strings.ToUpper(os.Getenv("minBlockDuration"))
	if len(minBlockDuration) == 0 {
		minBlockDuration = "1M"
	}
	minBlockDurationValue, err = strconv.Atoi(string(minBlockDuration[0 : len(minBlockDuration)-1]))
	if err == nil {
		minBlockDurationUnit = string(minBlockDuration[len(minBlockDuration)-1])
	}
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
	err = retry.Do(
		func() error {
			kafkaClient, err = sarama.NewClient(strings.Split(brokers, ","), config)
			if err != nil {
				log.Printf("Error new kafka client: %v", err)
				return err
			}

			admin, err = sarama.NewClusterAdminFromClient(kafkaClient)
			if err != nil {
				log.Printf("Error new kafka cluster admin: %v", err)
				return err
			}
			return nil
		},
	)

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
	hasMessage := false
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
			if _topicParOffset > 0 {
				hasMessage = true
			}
		}
	}
	if !hasMessage {
		succ = false
	}
	return succ, nil
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
		} else {
			log.Printf("closing  consumer(%s) success!", cgStr)
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
		return consumer.ConsumeJobNotifyEventMsg(session, claim)
	} else {
		return consumer.ConsumeAppMsg(session, claim)
	}

}

type CleanExpiredTopicJob struct {
	*BaseJob
	jobEndMessage *sarama.ConsumerMessage
	session       sarama.ConsumerGroupSession
}

func (s *CleanExpiredTopicJob) Execute() error {
	cleanFinishedTopic(s.session, s.jobEndMessage)
	return nil
}

type CleanTopicJob struct {
	*BaseJob
	jobEndMessage *sarama.ConsumerMessage
	session       sarama.ConsumerGroupSession
}

func (s *CleanTopicJob) Execute() error {
	ticker := time.NewTicker(30 * time.Second)
	topic := s.jobEndMessage.Topic + "-" + string(s.jobEndMessage.Key)

	succ := checkTopicConsumedFinished(topic)
	for {
		if succ {
			ticker.Stop()
			cleanFinishedTopic(s.session, s.jobEndMessage)
			break
		}
		<-ticker.C

		succ = checkTopicConsumedFinished(topic)

	}

	return nil
}

type StartComsumerJob struct {
	*BaseJob
	jobStartMessage *sarama.ConsumerMessage
	session         sarama.ConsumerGroupSession
}

func (s *StartComsumerJob) Execute() error {
	topic := s.jobStartMessage.Topic + "-" + string(s.jobStartMessage.Key)
	go func(topic string) {
		succ := checkTopicConsumedFinished(topic)
		_, readyExist := consumerGroupCache[topic]
		if !succ && !readyExist {
			//session.MarkMessage(job_started, "")

			CreateConsumer(topic, false)

		}

	}(topic)

	return nil
}

func cleanFinishedTopic(session sarama.ConsumerGroupSession, jobMessage *sarama.ConsumerMessage) {
	jobs_ended := jobMessage
	topic := jobs_ended.Topic
	topicName := topic + "-" + string(jobs_ended.Key)

	err = admin.DeleteTopic(topicName)

	if err == nil {
		log.Printf("DeleteTopic: %s", topicName)
	} else {
		log.Printf("DeleteTopic %s failed, %v ", topicName, err)
	}

	cleanPartionRecords(session, jobMessage)

	comsumer, cgReady := consumerGroupCache[topicName]
	if cgReady {
		//*ready <- false
		close((*comsumer).ready)
		delete(consumerGroupCache, topic)
	}
}

func cleanPartionRecords(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) {
	//session.MarkMessage(message, "")
	partitionOffsets := make(map[int32]int64)
	partitionOffsets[message.Partition] = message.Offset + 1
	err = admin.DeleteRecords(message.Topic, partitionOffsets)
	if err == nil {
		log.Printf("Delete topic: %s  records with partition: %d and offset: %d", message.Topic, message.Partition, message.Offset)
	} else {
		log.Printf("Error delete topic: %s  records: %v", message.Topic, err)
	}
}

func checkExpiredJob(epochMillis int64) bool {
	now := time.Now() //获取当前时间对象

	if minBlockDurationUnit == "M" {
		timestamp := now.Add(-1 * time.Duration(time.Minute*time.Duration(minBlockDurationValue)))
		ts := timestamp.UnixMilli()
		if ts > epochMillis {
			return true
		}

	} else if minBlockDurationUnit == "H" {
		timestamp := now.Add(-1 * time.Duration(time.Hour*time.Duration(minBlockDurationValue)))
		ts := timestamp.UnixMilli()
		if ts > epochMillis {
			return true
		}
	}

	return false
}

func (consumer *Consumer) ConsumeJobNotifyEventMsg(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var claimMessageChan = claim.Messages()
	var currentMessage *sarama.ConsumerMessage

	var topicQueueCache sync.Map

	for {
		select {
		case message, ok := <-claimMessageChan:
			if ok {
				currentMessage = message
				currentMessageValue := string(currentMessage.Value)
				_map, err := convertMap(currentMessageValue)
				if err == nil {
					name := GetInterfaceToString(_map["name"])

					_, currentKey, err := convertTimeSeries(_map)

					if err == nil && currentKey != "" && currentKey != "noAppId" {
						currentPartition := currentMessage.Partition
						if _, ok := topicQueueCache.Load(currentPartition); !ok {
							queue := &JobQueue{
								jobList:    list.New(),
								noticeChan: make(chan struct{}, 10),
							}
							//初始化一个消费worker
							workerManger := WorkerManager{queue, make(chan struct{}, 1)}
							// worker开始监听队列
							go workerManger.StartWork()
							topicQueueCache.Store(currentPartition, queue)

						}
						_workQueue, _ := topicQueueCache.Load(currentPartition)
						workQueue := _workQueue.(*JobQueue)

						if epochMillis, ok := _map["epochMillis"].(float64); ok {
							expiredJob := checkExpiredJob(int64(epochMillis))
							if !expiredJob {
								if name == "jobs_started" {
									// push job
									job := &StartComsumerJob{
										BaseJob: &BaseJob{
											DoneChan: make(chan struct{}, 1),
										},
										jobStartMessage: currentMessage,
										session:         session,
									}
									//压入队列尾部
									workQueue.PushJob(job)
								} else if name == "jobs_ended" {
									// push TopicCleanJob
									job := &CleanTopicJob{
										BaseJob: &BaseJob{
											DoneChan: make(chan struct{}, 1),
										},
										jobEndMessage: currentMessage,
										session:       session,
									}
									//压入队列尾部
									workQueue.PushJob(job)
								}
							} else {
								if name == "jobs_ended" {
									job := &CleanExpiredTopicJob{
										BaseJob: &BaseJob{
											DoneChan: make(chan struct{}, 1),
										},
										jobEndMessage: currentMessage,
										session:       session,
									}
									//压入队列尾部
									workQueue.PushJob(job)
								}
							}
						}

					}
					log.Printf("Message consumed: topic=%s, hash-key=%s, partition=%d, offset{%d}, message: %s",
						(*currentMessage).Topic, string((*currentMessage).Key), (*currentMessage).Partition, (*currentMessage).Offset, (*currentMessage).Value)
				}
			}

		case <-session.Context().Done():
			return nil

		}
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

				}

			}

		case <-time.After(time.Second * 3):
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

	//if err == nil {
	session.MarkMessage(currentMessage, "")
	//beginoffset := (*currentMessage).Offset - int64(len(tslist)) + 1
	//log.Printf("Message consumed: topic=%s,  partition=%d, from-to offset{%d--%d}", (*currentMessage).Topic, (*currentMessage).Partition, beginoffset, (*currentMessage).Offset)
	//}
	return nil
}
