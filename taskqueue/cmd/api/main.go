package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Payload     string `json:"payload"`
	Status      string `json:"status"`
	CompletedAt string `json:"completed_at,omitempty"` // only set when done
}

var (
	jobQueue []Job
	mu       sync.Mutex // to protect jobQueue
)

var jobQueueChanged bool

// For database
var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("sqlite", "jobs.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err := createJobsTable(); err != nil {
		log.Fatalf("Failed to create jobs table: %v", err)
	}

	if err := loadJobsFromFile(); err != nil {
		log.Fatalf("Failed to load jobs from file: %v", err)
	}

	http.HandleFunc("/jobs", handleJobSubmission) // This is a POST method
	http.HandleFunc("/jobs/view", handleJobList)  // This is to get all the jobs
	http.HandleFunc("/job/", handleJobByID)

	go startWorker()
	go startAutoSaver()

	fmt.Println("Starting server on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleJobSubmission(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received POST request to /jobs")
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode the incoming JSON
	var incoming struct {
		Type    string `json:"type"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Create a new Job with a unique ID
	job := Job{
		ID:      uuid.New().String(),
		Type:    incoming.Type,
		Payload: incoming.Payload,
		Status:  "pending",
	}

	mu.Lock()
	// Add the job to the queue
	jobQueue = append(jobQueue, job)
	if err := insertJobToDB(job); err != nil {
		log.Printf("Failed to save job to DB: %v", err)
	}
	jobQueueChanged = true
	mu.Unlock()

	// Respond with the created job
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func handleJobList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(jobQueue)

	jobs, err := getAllJobsFromDB()
	if err != nil {
		http.Error(w, "Failed to retrieve jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)

}

func startWorker() {
	for {
		time.Sleep(1 * time.Second) // check every 1 second

		mu.Lock()
		if len(jobQueue) > 0 {
			job := &jobQueue[0]     // Get pointer to the job
			jobQueue = jobQueue[1:] // Remove from queue
			mu.Unlock()

			fmt.Printf("Processing job: ID=%s, Type=%s, Payload=%s\n", job.ID, job.Type, job.Payload)
			time.Sleep(2 * time.Second) // simulate work

			job.Status = "completed"
			job.CompletedAt = time.Now().Format(time.RFC3339)
			jobQueueChanged = true
			err := updateJobStatusInDB(*job)
			if err != nil {
				log.Printf("Failed to update job in DB: %v", err)
			}
			fmt.Printf("Finished job: ID=%s, Status=%s, CompletedAt=%s\n", job.ID, job.Status, job.CompletedAt)
		} else {
			mu.Unlock()
		}
	}
}

func handleJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Path[len("/job/"):] // meaning of this is not clear
	mu.Lock()
	defer mu.Unlock()

	for _, job := range jobQueue {
		if job.ID == id {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(job)
			return
		}
	}

	http.Error(w, "Job not found", http.StatusNotFound)
}

func saveJobsToFile() error {
	data, err := json.MarshalIndent(jobQueue, "", "  ")
	if err != nil {
		return err
	}

	tmpFile := "jobs_temp.json"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, "jobs.json")

}

func loadJobsFromFile() error {
	file, err := os.Open("jobs.json")
	if err != nil {
		if os.IsNotExist(err) {
			jobQueue = []Job{} // file doesn’t exist yet, start fresh
			return nil
		}
		return err
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &jobQueue)
}

func startAutoSaver() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		mu.Lock()
		if jobQueueChanged {
			err := saveJobsToFile()
			if err != nil {
				fmt.Printf("Error saving jobs: %v\n", err)
			} else {
				jobQueueChanged = false
			}
		}
		mu.Unlock()
	}
}

func createJobsTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		type TEXT,
		payload TEXT,
		status TEXT,
		completed_at TEXT
	);`
	_, err := db.Exec(query)
	return err
}

func insertJobToDB(job Job) error {
	_, err := db.Exec(`INSERT INTO jobs (id, type, payload, status, completed_at)
		VALUES (?, ?, ?, ?, ?)`,
		job.ID, job.Type, job.Payload, job.Status, job.CompletedAt)
	return err
}

func updateJobStatusInDB(job Job) error {
	_, err := db.Exec(`
		UPDATE jobs
		SET status = ?, completed_at = ?
		WHERE id = ?`,
		job.Status, job.CompletedAt, job.ID)
	return err
}

func getAllJobsFromDB() ([]Job, error) {
	rows, err := db.Query("SELECT id, type, payload, status, completed_at FROM jobs")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job

	for rows.Next() {
		var job Job
		err := rows.Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CompletedAt)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil

}
