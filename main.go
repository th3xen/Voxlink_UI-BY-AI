package main

import (
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

//go:embed templates/index.html
var templatesFS embed.FS

// jobManager — глобальный менеджер задачи (одна задача за раз)
type jobManager struct {
	mu     sync.Mutex
	state  *State
	cancel context.CancelFunc
}

var manager = &jobManager{
	state: &State{Status: "idle"},
}

func main() {
	// Пробуем восстановить состояние после сбоя
	if saved, err := loadState(); err == nil && saved.Status == "running" {
		saved.Status = "interrupted"
		manager.state = saved
		log.Printf("Обнаружено прерванное задание: %s", saved.JobID)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/start", handleStart)
	mux.HandleFunc("/resume", handleResume)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/download", handleDownload)
	mux.HandleFunc("/cancel", handleCancel)

	port := "8080"
	url := "http://localhost:" + port

	log.Println("Сервер запущен:", url)

	go func() {
		time.Sleep(300 * time.Millisecond)
		openBrowser(url)
	}()

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

// handleIndex — главная страница
func handleIndex(w http.ResponseWriter, r *http.Request) {
	renderTemplate(w, manager.state.Snapshot())
}

// handleStart — загрузка файла и запуск обработки
func handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	manager.mu.Lock()
	if manager.state.Status == "running" {
		manager.mu.Unlock()
		http.Error(w, "задание уже выполняется", http.StatusConflict)
		return
	}
	manager.mu.Unlock()

	// Читаем загруженный файл
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "ошибка формы: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("inputFile")
	if err != nil {
		http.Error(w, "файл не найден: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Сохраняем загруженный файл во временную директорию
	tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("voxlink_input_%d.csv", time.Now().UnixNano()))
	tmp, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, "ошибка сохранения: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(tmp, file); err != nil {
		tmp.Close()
		http.Error(w, "ошибка записи: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmp.Close()

	// Путь для сохранения результата
	outputPath := strings.TrimSpace(r.FormValue("outputPath"))
	if outputPath == "" {
		outputPath = filepath.Join(os.TempDir(), fmt.Sprintf("voxlink_output_%d.csv", time.Now().UnixNano()))
	}

	// Считаем общее количество строк для прогресс-бара
	total, err := countLines(tmpPath)
	if err != nil {
		http.Error(w, "ошибка подсчёта строк: "+err.Error(), http.StatusInternalServerError)
		return
	}

	jobID := fmt.Sprintf("job_%d", time.Now().UnixNano())

	manager.mu.Lock()
	manager.state = &State{
		JobID:      jobID,
		InputFile:  tmpPath,
		OutputFile: outputPath,
		Total:      total,
		Processed:  0,
		Status:     "running",
	}
	manager.state.Save()
	manager.mu.Unlock()

	go runJob(manager.state, "")

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// handleResume — восстановление с последнего обработанного номера
func handleResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}

	manager.mu.Lock()
	s := manager.state
	if s.Status != "interrupted" && s.Status != "error" {
		manager.mu.Unlock()
		http.Error(w, "нечего восстанавливать", http.StatusBadRequest)
		return
	}
	lastNumber := s.LastNumber
	s.Status = "running"
	s.ErrorMsg = ""
	manager.mu.Unlock()

	go runJob(s, lastNumber)

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// handleStatus — JSON с текущим состоянием (для polling с фронта)
func handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snap := manager.state.Snapshot()

	percent := 0
	if snap.Total > 0 {
		percent = (snap.Processed * 100) / snap.Total
	}

	type response struct {
		State
		Percent int `json:"percent"`
	}

	json.NewEncoder(w).Encode(response{snap, percent})
}

// handleDownload — скачать результирующий файл
func handleDownload(w http.ResponseWriter, r *http.Request) {
	snap := manager.state.Snapshot()
	if snap.Status != "done" {
		http.Error(w, "файл ещё не готов", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Disposition", `attachment; filename="output.csv"`)
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	http.ServeFile(w, r, snap.OutputFile)
}

// handleCancel — отмена текущего задания
func handleCancel(w http.ResponseWriter, r *http.Request) {
	manager.mu.Lock()
	if manager.cancel != nil {
		manager.cancel()
	}
	manager.mu.Unlock()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// runJob — основной pipeline обработки
func runJob(s *State, resumeAfter string) {
	inputFile, err := os.Open(s.InputFile)
	if err != nil {
		s.mu.Lock()
		s.Status = "error"
		s.ErrorMsg = err.Error()
		s.mu.Unlock()
		s.Save()
		return
	}
	defer inputFile.Close()

	// Открываем output: если восстанавливаемся — дописываем, иначе создаём заново
	var outputFile *os.File
	if resumeAfter != "" {
		outputFile, err = os.OpenFile(s.OutputFile, os.O_APPEND|os.O_WRONLY, 0644)
	} else {
		outputFile, err = os.Create(s.OutputFile)
	}
	if err != nil {
		s.mu.Lock()
		s.Status = "error"
		s.ErrorMsg = err.Error()
		s.mu.Unlock()
		s.Save()
		return
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Пишем заголовок только при новом задании
	if resumeAfter == "" {
		writer.Write([]string{"Номер", "Статус", "Оператор", "Регион", "Код", "Полный номер", "Ошибка"})
	}

	reader := csv.NewReader(inputFile)
	client := &http.Client{Timeout: timeout}
	limiter := rate.NewLimiter(rate.Limit(rpsLimit), rpsLimit)

	jobs := make(chan string, workerCount*2)
	results := make(chan Result, workerCount*2)
	done := make(chan struct{}, workerCount)

	// Запускаем воркеры
	for i := 0; i < workerCount; i++ {
		go runWorker(client, limiter, jobs, results, done)
	}

	// Закрываем results когда все воркеры завершились
	go func() {
		for i := 0; i < workerCount; i++ {
			<-done
		}
		close(results)
	}()

	// Читаем CSV и отправляем в jobs, пропуская до resumeAfter
	skipping := resumeAfter != ""
	go func() {
		defer close(jobs)
		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil || len(row) == 0 {
				continue
			}
			number := strings.TrimSpace(row[0])
			if number == "" {
				continue
			}
			// Пропускаем уже обработанные строки при восстановлении
			if skipping {
				if number == resumeAfter {
					skipping = false
				}
				continue
			}
			jobs <- number
		}
	}()

	// Пишем результаты
	for r := range results {
		writer.Write([]string{
			r.Number, r.Status, r.Operator,
			r.Region, r.Code, r.Full, r.Error,
		})

		s.mu.Lock()
		s.Processed++
		if r.Error == "" {
			s.LastNumber = r.Number
		} else {
			s.Errors++
		}
		s.mu.Unlock()

		// Периодически сохраняем состояние
		if s.Processed%50 == 0 {
			writer.Flush()
			s.Save()
		}
	}

	s.mu.Lock()
	s.Status = "done"
	s.mu.Unlock()
	s.Save()
}

// countLines — считаем строки CSV для прогресс-бара
func countLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	count := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		count++
	}
	return count, nil
}

// renderTemplate — рендер HTML с embed
func renderTemplate(w http.ResponseWriter, data State) {
	funcMap := template.FuncMap{
		"pct": func(processed, total int) int {
			if total == 0 {
				return 0
			}
			return (processed * 100) / total
		},
	}

	tmpl := template.Must(
		template.New("index.html").Funcs(funcMap).ParseFS(templatesFS, "templates/index.html"),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, data)
}

// openBrowser — открывает браузер по умолчанию
func openBrowser(url string) {
	var err error

	switch runtime.GOOS {
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	default:
		log.Printf("Откройте браузер вручную: %s", url)
		return
	}

	if err != nil {
		log.Printf("Не удалось открыть браузер: %v", err)
	}
}
