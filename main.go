package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type fnToProcess func() (string, error)

type Scheduler struct {
	tasks       []fnToProcess
	wg          *sync.WaitGroup
	threadCount int
	result      []string
	ch          chan fnToProcess
	errChan     chan error
	semChan     chan struct{}
	resultChan  chan string
	endChannel  chan struct{}
}

func createHeavyTask(i int, isErr bool) fnToProcess {
	return func() (string, error) {

		if isErr {
			return fmt.Sprintf("err %d", i), errors.New("errors happened")
		}
		time.Sleep(time.Microsecond * 1)

		return fmt.Sprintf("done %d", i), nil
	}

}

func CreateScheduler() Scheduler {
	var wg sync.WaitGroup
	threadCount := 4
	semChan := make(chan struct{}, threadCount)
	endChan := make(chan struct{})
	ch := make(chan fnToProcess, 0)
	result := make([]string, 0)
	resultChan := make(chan string)

	sched := Scheduler{
		wg:          &wg,
		semChan:     semChan,
		threadCount: threadCount,
		result:      result,
		ch:          ch,
		resultChan:  resultChan,
		endChannel:  endChan,
	}
	return sched
}

func (self *Scheduler) AddTask(fn fnToProcess) {
	self.tasks = append(self.tasks, fn)
}
func (self *Scheduler) Start() {
	self.wg.Add(1)
	go func() {
		defer func() {
			close(self.ch)
			self.wg.Done()

		}()
		for i := range self.tasks {
			select {
			case self.ch <- self.tasks[i]:
			case <-self.endChannel:
				return
			}
		}
	}()

	go func() {
		for fn := range self.ch {
			self.semChan <- struct{}{}
			self.wg.Add(1)
			go func(fn fnToProcess) {
				defer func() {
					<-self.semChan
					self.wg.Done()
				}()
				for {
					select {
					default:
						res, err := fn()
						if err != nil {
							defer func() {
								close(self.endChannel)
							}()
							return
						}
						select {
						default:
							self.resultChan <- res
						case <-self.endChannel:
						}
						return

					case <-self.endChannel:
						return
					}

				}

			}(fn)
		}
	}()
	go func() {
		self.wg.Wait()
		close(self.resultChan)
	}()
	for result := range self.resultChan {
		fmt.Println(result)
	}
}

func main() {

	scheduler := CreateScheduler()
	for i := 0; i < 10000; i++ {
		scheduler.AddTask(createHeavyTask(i, i == 6000))
	}
	scheduler.Start()

}
