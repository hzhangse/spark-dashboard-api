package utils

import (
	"container/list"
	"context"
	"sync"
)

type JobQueue struct {
	mu         sync.Mutex    //队列的操作需要并发安全
	jobList    *list.List    //List是golang库的双向队列实现，每个元素都是一个job
	noticeChan chan struct{} //入队一个job就往该channel中放入一个消息，以供消费者消费
}

/**
 * 队列的Push操作
 */
func (queue *JobQueue) PushJob(job Job) {
	queue.jobList.PushBack(job) //将job加到队尾
	queue.noticeChan <- struct{}{}
}

/**
 * 弹出队列的第一个元素
 */
func (queue *JobQueue) PopJob() Job {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	/**
	 * 说明在队列中没有元素了
	 */
	if queue.jobList.Len() == 0 {
		return nil
	}
	elements := queue.jobList.Front()           //获取队里的第一个元素
	return queue.jobList.Remove(elements).(Job) //将元素从队列中移除并返回
}

func (queue *JobQueue) WaitJob() <-chan struct{} {
	return queue.noticeChan
}

type Job interface {
	Execute() error
	WaitDone()
	Done()
}

type BaseJob struct {
	Err        error
	DoneChan   chan struct{} //当作业完成时，或者作业被取消时，通知调用者
	Ctx        context.Context
	cancelFunc context.CancelFunc
}

/**
 * 作业执行完毕，关闭DoneChan，所有监听DoneChan的接收者都能收到关闭的信号
 */
func (job *BaseJob) Done() {
	close(job.DoneChan)
}

/**
 * 等待job执行完成
 */
func (job *BaseJob) WaitDone() {
	select {
	case <-job.DoneChan:
		return
	}
}

type WorkerManager struct {
	queue     *JobQueue
	closeChan chan struct{}
}

// StartWorker函数，只有一个for循环，不断的从队列中获取Job。获取到Job后，进行消费Job，即ConsumeJob。
func (m *WorkerManager) StartWork() error {
	for {
		select {
		case <-m.closeChan:
			return nil
		case <-m.queue.noticeChan:
			job := m.queue.PopJob()
			m.ConsumeJob(job)
		}
	}
	return nil
}

func (m *WorkerManager) ConsumeJob(job Job) {
	defer func() {
		job.Done()
	}()
	job.Execute()
}
