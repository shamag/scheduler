package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Scheduler struct {
	tasks       []func() (string, error)
	wg          *sync.WaitGroup
	threadCount int
	result      []string
	ch          chan func() (string, error)
	errChan     chan error
	semChan     chan struct{}
	resultChan  chan string
	endChannel  chan struct{}
}

func createHeavyTask(i int) func() (string, error) {
	return func() (string, error) {
		fmt.Printf("Запуск функции %d \n", i)
		//isErr := rand.Int31n(3)

		if i == 5004 {
			fmt.Printf("error happens fn %d \n", i)
			return "", errors.New("errors happened")
		}
		time.Sleep(time.Millisecond)
		fmt.Printf("завершение функции %d \n", i)
		return fmt.Sprintf("done %d", i), nil
	}

}

func CreateScheduler() Scheduler {
	var wg sync.WaitGroup
	threadCount := 12
	semChan := make(chan struct{}, threadCount)
	endChan := make(chan struct{})
	errChan := make(chan error)
	ch := make(chan func() (string, error))
	result := make([]string, 0)
	resultChan := make(chan string)

	sched := Scheduler{
		wg:          &wg,
		semChan:     semChan,
		threadCount: 8,
		result:      result,
		errChan:     errChan,
		ch:          ch,
		resultChan:  resultChan,
		endChannel:  endChan,
	}
	return sched
}

func (self *Scheduler) AddTask(fn func() (string, error)) {
	self.tasks = append(self.tasks, fn)
}
func (self *Scheduler) Start() {
	go func() {
		for i := range self.tasks {
			self.ch <- self.tasks[i]
		}
		close(self.ch)
	}()

	self.wg.Add(1)
	// println("старт")
	go func() {
		for fn := range self.ch {
			self.semChan <- struct{}{}
			self.wg.Add(1)
			go func(fn func() (string, error)) {
				res, err := fn()
				if err != nil {
					self.errChan <- err
					return
				}
				self.resultChan <- res
				<-self.semChan
				self.wg.Done()
				return

			}(fn)
		}
		self.wg.Done()
	}()
	go func() {
		self.wg.Wait()
		//fmt.Println("завершение")

		self.endChannel <- struct{}{}

	}()
	//fmt.Println("Запуск слушателей")
label:
	for {
		select {
		case err := <-self.errChan:
			fmt.Println(err)
			break label
		case <-self.endChannel:
			fmt.Println("end")
			break label
		case res := <-self.resultChan:
			fmt.Println(res)
		}
	}
}

func main() {

	scheduler := CreateScheduler()
	for i := 0; i < 10000; i++ {
		scheduler.AddTask(createHeavyTask(i))
	}
	scheduler.Start()
}
