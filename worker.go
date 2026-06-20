package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiURL      = "https://num.voxlink.ru/get/"
	timeout     = 5 * time.Second
	rpsLimit    = 10
	workerCount = 5
)

type VoxlinkResponse struct {
	Status   string `json:"status"`
	Operator string `json:"operator"`
	Region   string `json:"region"`
	Code     string `json:"code"`
	Number   string `json:"number"`
	Error    string `json:"error"`
}

type Result struct {
	Number   string
	Status   string
	Operator string
	Region   string
	Code     string
	Full     string
	Error    string
}

func runWorker(
	client *http.Client,
	limiter *rate.Limiter,
	jobs <-chan string,
	results chan<- Result,
	done chan<- struct{},
) {
	defer func() { done <- struct{}{} }()

	for number := range jobs {
		if err := limiter.Wait(context.Background()); err != nil {
			results <- Result{Number: number, Error: err.Error()}
			continue
		}
		results <- processNumber(client, number)
	}
}

func processNumber(client *http.Client, number string) Result {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	u, _ := url.Parse(apiURL)
	q := u.Query()
	q.Set("num", number)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Result{Number: number, Error: err.Error()}
	}
	req.Header.Set("User-Agent", "voxlink-go-client/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return Result{Number: number, Error: err.Error()}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{Number: number, Error: err.Error()}
	}

	if resp.StatusCode != http.StatusOK {
		return Result{Number: number, Error: fmt.Sprintf("http %d: %s", resp.StatusCode, body)}
	}

	if !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		return Result{Number: number, Error: fmt.Sprintf("unexpected content-type: %s", resp.Header.Get("Content-Type"))}
	}

	var apiResp VoxlinkResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return Result{Number: number, Error: fmt.Sprintf("json: %v", err)}
	}

	return Result{
		Number:   number,
		Status:   apiResp.Status,
		Operator: apiResp.Operator,
		Region:   apiResp.Region,
		Code:     apiResp.Code,
		Full:     apiResp.Number,
		Error:    apiResp.Error,
	}
}
