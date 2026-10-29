package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Metric struct {
	Source string
	Value  float64
	Time   time.Time
}

func cpuMetrics() <-chan Metric {
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			ch <- Metric{
				Source: "CPU",
				Value:  rand.Float64() * 100,
				Time:   time.Now(),
			}
			time.Sleep(800 * time.Millisecond)
		}
	}()
	return ch
}

func memoryMetrics() <-chan Metric {
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			ch <- Metric{
				Source: "Memory",
				Value:  rand.Float64() * 16384,
				Time:   time.Now(),
			}
			time.Sleep(1200 * time.Millisecond)
		}
	}()
	return ch
}

func networkMetrics() <-chan Metric {
	ch := make(chan Metric)
	go func() {
		defer close(ch)
		for i := 0; i < 5; i++ {
			ch <- Metric{
				Source: "Network",
				Value:  rand.Float64() * 1000,
				Time:   time.Now(),
			}
			time.Sleep(1500 * time.Millisecond)
		}
	}()
	return ch
}

func fanIn(channels ...<-chan Metric) <-chan Metric {
	out := make(chan Metric)
	var wg sync.WaitGroup

	// создаем по горутине на каждый полученный канал. горутина читает переданный ей канал и передает данные в общий канал
	for _, ch := range channels {
		wg.Add(1)
		go func(c <-chan Metric) {
			defer wg.Done()
			for m := range c {
				out <- m
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Запуск системы мониторинга…")

	// создаем каналы метрик.
	cpu := cpuMetrics()
	mem := memoryMetrics()
	net := networkMetrics()

	// объединяем все каналы в один.
	merged := fanIn(cpu, mem, net)

	// читаем объединённый канал до его закрытия
	for metric := range merged {
		fmt.Printf("Источник: %s, Значение: %.2f, Время: %s\n",
			metric.Source, metric.Value, metric.Time.Format("15:04:05.000"))
	}

	fmt.Println("Мониторинг завершен.")
}
