package main

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type Scheduler struct {
	tasks       []func(bool) (string, error)
	wg          *sync.WaitGroup
	threadCount int
	result      []string
	ch          chan func(bool) (string, error)
	errChan     chan error
	semChan     chan struct{}
	resultChan  chan string
	endChannel  chan struct{}
}

func createHeavyTask(i int, isErr bool) func(bool) (string, error) {
	return func(isInstant bool) (string, error) {
		if !isInstant {
			fmt.Printf("Запуск функции %d \n", i)
		}
		//isErr := rand.Int31n(3)

		if isErr {
			fmt.Printf("error happens fn %d \n", i)
			return fmt.Sprintf("err %d", i), errors.New("errors happened")
		}
		if !isInstant {
			time.Sleep(time.Microsecond * 1)
			fmt.Printf("завершение функции %d \n", i)
		}

		return fmt.Sprintf("done %d", i), nil
	}

}

func CreateScheduler() Scheduler {
	var wg sync.WaitGroup
	threadCount := 4
	semChan := make(chan struct{}, threadCount)
	endChan := make(chan struct{})
	ch := make(chan func(bool) (string, error), 0)
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

func (self *Scheduler) AddTask(fn func(bool) (string, error)) {
	self.tasks = append(self.tasks, fn)
}
func (self *Scheduler) Start() {
	self.wg.Add(1)
	go func() {
		defer func() {
			close(self.ch)
			self.wg.Done()
			fmt.Println("xxxxxxx")

		}()
		for i := range self.tasks {
			fmt.Println("iter", i)
			select {
			case self.ch <- self.tasks[i]:
				fmt.Println("task chan", i)
			case <-self.endChannel:
				fmt.Println("err taken 2")
				return
			}
		}
	}()

	// println("старт")
	go func() {
		for fn := range self.ch {
			self.semChan <- struct{}{}
			self.wg.Add(1)
			go func(fn func(bool) (string, error)) {
				fmt.Println("start executor")
				defer func() {
					// fmt.Println("close executor")
					<-self.semChan
					self.wg.Done()
					fmt.Println("sem freed")
				}()
				for {
					select {
					default:
						dbg, _ := fn(true)
						fmt.Println("fn taken", dbg)
						res, err := fn(false)
						if err != nil {
							// fmt.Println("err taken")
							defer func() {
								close(self.endChannel)
								fmt.Println("err sended")
							}()
							return
						}
						runtime.Gosched()
						self.resultChan <- res
						fmt.Println("result sended")
						return

					case <-self.endChannel:
						res, _ := fn(true)
						fmt.Println("err taken 3", res)
						return
					}

				}

			}(fn)
		}
	}()
	go func() {
		self.wg.Wait()
		close(self.resultChan)
		fmt.Println("closedResult")
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

	time.Sleep(5 * time.Second)
}
