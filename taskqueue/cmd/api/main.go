package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	_ "modernc.org/sqlite"
)

type Job struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Payload     string `json:"payload"`
	Status      string `json:"status"`
	CompletedAt string `json:"completed_at,omitempty"` // only set when done
}

// var (
// 	jobQueue []Job
// 	mu       sync.Mutex // to protect jobQueue
// )

// var jobQueueChanged bool

// For database
var db *sql.DB

var jobsChan = make(chan Job)

func main() {
	var err error
	db, err = sql.Open("sqlite", "jobs.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err := createJobsTable(); err != nil {
		log.Fatalf("Failed to create jobs table: %v", err)
	}

	// if err := loadJobsFromFile(); err != nil {
	// 	log.Fatalf("Failed to load jobs from file: %v", err)
	// }

	http.HandleFunc("/jobs", handleJobSubmission) // This is a POST method
	http.HandleFunc("/jobs/view", handleJobList)  // This is to get all the jobs
	http.HandleFunc("/job/", handleJobByID)

	startWorkerPool(5)
	go startDispatcher()
	//go startAutoSaver()

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

	// mu.Lock()
	// // Add the job to the queue
	// jobQueue = append(jobQueue, job)
	if err := insertJobToDB(job); err != nil {
		log.Printf("Failed to save job to DB: %v", err)
	}
	// jobQueueChanged = true
	// mu.Unlock()

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

		//mu.Lock()
		job, err := fetchNextPendingJob()
		if err != nil {
			fmt.Printf("Error fetching job: %v\n", err)
			continue
		}
		if job == nil {
			continue // no pending job
		}

		fmt.Printf("Processing job: ID=%s, Type=%s, Payload=%s\n", job.ID, job.Type, job.Payload)
		time.Sleep(2 * time.Second) // simulate work

		err = updateJobStatus(job.ID, "completed", time.Now().Format(time.RFC3339))
		if err != nil {
			fmt.Printf("Error updating job: %v\n", err)
		} else {
			fmt.Printf("Finished job: ID=%s, Status=completed\n", job.ID)
		}

		// if len(jobQueue) > 0 {
		// 	job := &jobQueue[0]     // Get pointer to the job
		// 	jobQueue = jobQueue[1:] // Remove from queue
		// 	mu.Unlock()

		// 	fmt.Printf("Processing job: ID=%s, Type=%s, Payload=%s\n", job.ID, job.Type, job.Payload)
		// 	time.Sleep(2 * time.Second) // simulate work

		// 	job.Status = "completed"
		// 	job.CompletedAt = time.Now().Format(time.RFC3339)
		// 	jobQueueChanged = true
		// 	err := updateJobStatusInDB(*job)
		// 	if err != nil {
		// 		log.Printf("Failed to update job in DB: %v", err)
		// 	}
		// 	fmt.Printf("Finished job: ID=%s, Status=%s, CompletedAt=%s\n", job.ID, job.Status, job.CompletedAt)
		// } else {
		// 	mu.Unlock()
		// }
	}
}

func handleJobByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Path[len("/job/"):] // meaning of this is not clear

	// mu.Lock()
	// defer mu.Unlock()

	// for _, job := range jobQueue {
	// 	if job.ID == id {
	// 		w.Header().Set("Content-Type", "application/json")
	// 		json.NewEncoder(w).Encode(job)
	// 		return
	// 	}
	// }

	// http.Error(w, "Job not found", http.StatusNotFound)

	////This is from db

	job, err := getJobByIDFromDB(id)
	if err != nil {
		http.Error(w, "Failed to fetch job", http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// func saveJobsToFile() error {
// 	data, err := json.MarshalIndent(jobQueue, "", "  ")
// 	if err != nil {
// 		return err
// 	}

// 	tmpFile := "jobs_temp.json"
// 	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
// 		return err
// 	}
// 	return os.Rename(tmpFile, "jobs.json")

// }

// func loadJobsFromFile() error {
// 	file, err := os.Open("jobs.json")
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			jobQueue = []Job{} // file doesn’t exist yet, start fresh
// 			return nil
// 		}
// 		return err
// 	}
// 	defer file.Close()

// 	data, err := ioutil.ReadAll(file)
// 	if err != nil {
// 		return err
// 	}
// 	return json.Unmarshal(data, &jobQueue)
// }

// func startAutoSaver() {
// 	ticker := time.NewTicker(2 * time.Second)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		mu.Lock()
// 		if jobQueueChanged {
// 			err := saveJobsToFile()
// 			if err != nil {
// 				fmt.Printf("Error saving jobs: %v\n", err)
// 			} else {
// 				jobQueueChanged = false
// 			}
// 		}
// 		mu.Unlock()
// 	}
// }

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

func getJobByIDFromDB(id string) (*Job, error) {
	row := db.QueryRow("SELECT id,type,payload,status, completed_at FROM jobs WHERE id = ?", id)
	var job Job
	err := row.Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CompletedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // no job found
		}
		return nil, err
	}
	return &job, nil
}

func fetchNextPendingJob() (*Job, error) {

	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`
		SELECT id, type, payload, status, completed_at
		FROM jobs
		WHERE status = 'pending'
		LIMIT 1
	`)

	var job Job
	err = row.Scan(&job.ID, &job.Type, &job.Payload, &job.Status, &job.CompletedAt)
	if err != nil {
		tx.Rollback()
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	_, err = tx.Exec(`UPDATE jobs SET status = 'in_progress' WHERE id = ?`, job.ID)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &job, nil
}
func updateJobStatus(id, status, completedAt string) error {
	var err error
	for i := 0; i < 5; i++ {
		_, err = db.Exec(`
			UPDATE jobs
			SET status = ?, completed_at = ?
			WHERE id = ?
		`, status, completedAt, id)
		if err == nil {
			return nil
		}
		if err.Error() == "database is locked (5) (SQLITE_BUSY)" {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond) // simple backoff
			continue
		}
		return err // any other error, fail fast
	}
	return fmt.Errorf("updateJobStatus failed after retries: %w", err)
}

func startWorkerPool(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func(workerID int) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Worker %d] PANIC: %v", workerID, r)
				}
			}()
			for job := range jobsChan {
				log.Printf("[Worker %d] Started job ID=%s", workerID, job.ID)
				time.Sleep(2 * time.Second)

				err := updateJobStatus(job.ID, "completed", time.Now().Format(time.RFC3339))
				if err != nil {
					log.Printf("[Worker %d] Error updating job ID=%s: %v", workerID, job.ID, err)
				} else {
					log.Printf("[Worker %d] Finished job ID=%s", workerID, job.ID)
				}
			}
		}(i + 1)
	}
}

func startDispatcher() {
	for {
		job, err := fetchNextPendingJob()
		if err != nil {
			log.Printf("Dispatcher error fetching job: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if job == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		jobsChan <- *job
	}
}
