package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Определение типа функции, которые будет выполняться планировщиком
type fnToProcess func() (string, error)

type Scheduler struct {
	tasks       []fnToProcess
	wg          *sync.WaitGroup
	threadCount int
	result      []string
	ch          chan fnToProcess
	semChan     chan struct{}
	resultChan  chan string
	endChannel  chan struct{}
}

// Создание функции которая будет выполняться планировщиком
func createHeavyTask(i int, isErr bool) fnToProcess {
	return func() (string, error) {

		if isErr {
			return fmt.Sprintf("err %d", i), errors.New("errors happened")
		}
		time.Sleep(time.Millisecond * 5)

		return fmt.Sprintf("done %d", i), nil
	}

}

func CreateScheduler(threads int) Scheduler {
	var wg sync.WaitGroup
	semChan := make(chan struct{}, threads)
	endChan := make(chan struct{})
	ch := make(chan fnToProcess, 0)
	resultChan := make(chan string)

	sched := Scheduler{
		// для ожидания окончания выполнения
		wg: &wg,
		// канал для ограничения одновременно выполняющихся заданий
		semChan: semChan,
		// количество одновременно выполняющихся заданий
		threadCount: threads,
		// канал в котроый складываются задания для выполнения
		ch: ch,
		// канал для результатов выполнения заданий
		resultChan: resultChan,
		// канал для остановки выполнения
		endChannel: endChan,
	}
	return sched
}

func (self *Scheduler) AddTask(fn fnToProcess) {
	self.tasks = append(self.tasks, fn)
}
func (self *Scheduler) Start() {
	self.wg.Add(1)
	// Отправляем а канал self.ch функции
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

	// обработка заданий
	go func() {
		// каждое задание в своей горутине
		for fn := range self.ch {
			// но одновременно не более чем threadCount
			self.semChan <- struct{}{}
			self.wg.Add(1)
			go func(fn fnToProcess) {
				defer func() {
					//освобождаем занятый слот
					<-self.semChan
					self.wg.Done()
				}()

				//
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
		close(self.semChan)
	}()
	for result := range self.resultChan {
		fmt.Println(result)
	}
}

func main() {

	scheduler := CreateScheduler(10)
	for i := 0; i < 10000; i++ {
		scheduler.AddTask(createHeavyTask(i, i == 4000))
	}
	scheduler.Start()

}
