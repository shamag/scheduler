package main

import (
	"fmt"
	"sync"
)

type Scheduler struct {
	tasks []func() (string, error)
	wg sync.WaitGroup
	threadCount int
	result []string
	ch chan func() (string, error)
	errChan chan error
	semChan chan struct{}
	resultChan chan string
	endChannel chan struct{}
}

func heavyTask () (string, error) {
	return "done", nil
}

func CreateScheduler () Scheduler {
	var wg sync.WaitGroup
	threadCount := 4
	semChan := make(chan struct{}, threadCount)
	endChan := make(chan struct{})
	errChan := make(chan error)
	ch := make(chan func() (string, error))
	result := make([]string, 0)
	tasks := make([]func() (string, error), 0)
	resultChan := make(chan string)
	for i:=0;i<10 ;i++  {
		tasks = append(tasks, heavyTask)
	}


	sched := Scheduler{
		wg: wg,
		semChan: semChan,
		threadCount: 4,
		result: result,
		errChan: errChan,
		ch: ch,
		tasks: tasks,
		resultChan: resultChan,
		endChannel: endChan,
	}
	return sched
}
func (self *Scheduler) Start() {
	go func() {
		for i := range(self.tasks) {
			self.ch <- self.tasks[i]
		}
	}()

	self.wg.Add(1)
	println("старт")
	go func() {
		for fn := range self.ch {
			self.semChan <- struct{}{}
			self.wg.Add(1)
			go func(fn func() (string, error) ) {
				fmt.Println("Запуск функции")
				res, err := fn()
				if err != nil {
					self.errChan <- err
				}
				self.resultChan <- res
				<-self.semChan
				self.wg.Done()
			}(fn)
		}
		self.wg.Done()
	}()
	go func() {
		self.wg.Wait()
		fmt.Println("завершение")
		self.endChannel <- struct{}{}

	}()
	fmt.Println("Запуск слушателей")
	label: for ;;{
		select {
			case err:=<-self.errChan:
				fmt.Println(err)
			    break label
			case <- self.endChannel:
				fmt.Println("end")
				break label
			case res:=<-self.resultChan:
				fmt.Println(res)
				break label
		}
	}
}

func main () {
	scheduler := CreateScheduler ()
	scheduler.Start()
}