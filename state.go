package main

import (
	"encoding/json"
	"os"
	"sync"
)

// State — состояние текущей обработки
type State struct {
	mu sync.Mutex

	JobID      string `json:"jobId"`
	InputFile  string `json:"inputFile"`
	OutputFile string `json:"outputFile"`
	Total      int    `json:"total"`
	Processed  int    `json:"processed"`
	Errors     int    `json:"errors"`
	LastNumber string `json:"lastNumber"` // последний успешно обработанный номер
	Status     string `json:"status"`     // idle | running | done | error
	ErrorMsg   string `json:"errorMsg"`
}

const stateFile = "job_state.json"

func (s *State) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Create(stateFile)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(s)
}

func (s *State) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return State{
		JobID:      s.JobID,
		InputFile:  s.InputFile,
		OutputFile: s.OutputFile,
		Total:      s.Total,
		Processed:  s.Processed,
		Errors:     s.Errors,
		LastNumber: s.LastNumber,
		Status:     s.Status,
		ErrorMsg:   s.ErrorMsg,
	}
}

func loadState() (*State, error) {
	f, err := os.Open(stateFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var s State
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}
