// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A simple command-line task list manager to demonstrate using the
// cloud.google.com/go/datastore package.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

func main() {
	creds, err := parseCreds()
	if err != nil {
		log.Fatalf("failed to parse creds: %s", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting datastore task list on port %s", port)

	ctx := context.Background()
	client, err := datastore.NewClient(ctx, datastore.DetectProjectID, option.WithCredentials(creds))
	if err != nil {
		log.Fatalf("Could not create datastore client: %v", err)
	}

	log.Fatal(http.ListenAndServe(":"+port, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			// List
			tasks, err := ListTasks(ctx, client)
			if err != nil {
				log.Printf("failed to read from datastore: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to read from datastore: %s", err)
				return
			}
			json.NewEncoder(w).Encode(tasks)
		case http.MethodPost:
			// New
			data, err := readMsg(r.Body)
			if err != nil {
				log.Printf("failed to read message: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to read message: %s", err)
				return
			}

			key, err := AddTask(ctx, client, data)
			if err != nil {
				log.Printf("failed to create task: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to create task: %s", err)
				return
			}
			fmt.Fprintf(w, "created new task with ID %d\n", key.ID)
		case http.MethodDelete:
			// Delete
			idStr, err := readMsg(r.Body)
			if err != nil {
				log.Printf("failed to read message: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to read message: %s", err)
				return
			}
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "failed to parse ID (must be int64): %s", err)
				return
			}

			if err := MarkDone(ctx, client, id); err != nil {
				log.Printf("failed to mark task done: %s", err)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "failed to mark task done: %s", err)
			}
			fmt.Fprintf(w, "task %d marked done\n", id)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	})))
}

func parseCreds() (*google.Credentials, error) {
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		return nil, errors.New("SERVICE_NAME is required. It tells us which to use to connect to datastore")
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(os.Getenv("VCAP_SERVICES")), &m); err != nil {
		return nil, err
	}

	c, ok := m[serviceName].(map[string]interface{})["credentials"]
	if !ok {
		return nil, fmt.Errorf("%s service does not have credentials", serviceName)
	}

	jc, err := base64.StdEncoding.DecodeString(c.(map[string]interface{})["PrivateKeyData"].(string))
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode credentials: %s", err)
	}

	creds, err := google.CredentialsFromJSON(context.Background(), jc, "https://www.googleapis.com/auth/datastore")
	if err != nil {
		return nil, err
	}

	return creds, nil
}

// [START datastore_add_entity]
// Task is the model used to store tasks in the datastore.
type Task struct {
	Desc    string    `datastore:"description"`
	Created time.Time `datastore:"created"`
	Done    bool      `datastore:"done"`
	Id      int64     `datastore:"id"` // The integer ID used in the datastore.
}

// AddTask adds a task with the given description to the datastore,
// returning the key of the newly created entity.
func AddTask(ctx context.Context, client *datastore.Client, desc string) (*datastore.Key, error) {
	task := &Task{
		Desc:    desc,
		Created: time.Now(),
	}
	key := datastore.IncompleteKey("Task", nil)
	return client.Put(ctx, key, task)
}

// [END datastore_add_entity]

// [START datastore_update_entity]
// MarkDone marks the task done with the given ID.
func MarkDone(ctx context.Context, client *datastore.Client, taskID int64) error {
	// Create a key using the given integer ID.
	key := datastore.IDKey("Task", taskID, nil)

	// In a transaction load each task, set done to true and store.
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var task Task
		if err := tx.Get(key, &task); err != nil {
			return err
		}
		task.Done = true
		_, err := tx.Put(key, &task)
		return err
	})
	return err
}

// [END datastore_update_entity]

// [START datastore_retrieve_entities]
// ListTasks returns all the tasks in ascending order of creation time.
func ListTasks(ctx context.Context, client *datastore.Client) ([]*Task, error) {
	var tasks []*Task

	// Create a query to fetch all Task entities, ordered by "created".
	query := datastore.NewQuery("Task").Order("created")
	keys, err := client.GetAll(ctx, query, &tasks)
	if err != nil {
		return nil, err
	}

	// Set the id field on each Task from the corresponding key.
	for i, key := range keys {
		tasks[i].Id = key.ID
	}

	return tasks, nil
}

// [END datastore_retrieve_entities]

// [START datastore_delete_entity]
// DeleteTask deletes the task with the given ID.
func DeleteTask(ctx context.Context, client *datastore.Client, taskID int64) error {
	return client.Delete(ctx, datastore.IDKey("Task", taskID, nil))
}

func readMsg(r io.Reader) (string, error) {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, r, 256); err != nil && err != io.EOF {
		return "", err
	}

	return buf.String(), nil
}
