package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// задание для обработки
type Job struct {
	ID  int
	URL string
}

// результат обработки задания
type Result struct {
	Job      Job
	Status   string
	Duration time.Duration
}

// обрабатывает задания из канала jobs и отправляет результаты в канал results
func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		start := time.Now()

		// имитация запроса со случайной задержкой (100-500 мс)
		sleepDuration := time.Duration(100+rand.Intn(400)) * time.Millisecond
		time.Sleep(sleepDuration)

		duration := time.Since(start)

		// отправляем результаты в канал
		results <- Result{
			Job:      job,
			Status:   "обработан",
			Duration: duration,
		}
	}
}

func main() {
	// урлы для обработки
	urls := []string{
		"https://example1.com",
		"https://example2.com",
		"https://example3.org",
		"https://example4.com",
		"https://example5.com",
		"https://example6.com",
		"https://example7.com",
		"https://example8.com",
		"https://example9.com",
		"https://example10.com",
	}

	const numWorkers = 5

	jobs := make(chan Job, len(urls))
	results := make(chan Result, len(urls))

	var wg sync.WaitGroup

	// запускаем воркеров
	fmt.Printf("Запуск %d воркеров...\n", numWorkers)
	for range numWorkers {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// отправляем задания в канал jobs
	for i, url := range urls {
		jobs <- Job{
			ID:  i + 1,
			URL: url,
		}
	}
	close(jobs)

	// закрываем канал results после завершения всех воркеров
	go func() {
		wg.Wait()
		close(results)
	}()

	// собираем результаты
	var allResults []Result
	for result := range results {
		allResults = append(allResults, result)
	}

	// выводим отчёт
	printReport(allResults)
}

// отчёт по результатам обработки
func printReport(results []Result) {
	fmt.Println("Отчет")

	var totalDuration time.Duration

	for _, r := range results {
		fmt.Printf("Worker ID: %2d %-30s | Статус: %s | Время: %v\n",
			r.Job.ID, r.Job.URL, r.Status, r.Duration.Round(time.Millisecond))
		totalDuration += r.Duration
	}

	// Статистика
	fmt.Printf("Всего обработано URL:    %d\n", len(results))
	fmt.Printf("Общее время обработки:   %v\n", totalDuration.Round(time.Millisecond))

	if len(results) > 0 {
		avgDuration := totalDuration / time.Duration(len(results))
		fmt.Printf("Среднее время на запрос: %v\n", avgDuration.Round(time.Millisecond))
	}
}
