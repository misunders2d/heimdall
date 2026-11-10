package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/misunders2d/heimdall/internal/durable"
	"github.com/misunders2d/heimdall/internal/scheduler"
)

func schedControl(args []string) error {
	fs := flag.NewFlagSet("sched "+args[0], flag.ContinueOnError)
	socket := fs.String("socket", "", "local scheduler control socket")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *socket == "" || fs.NArg() != 0 {
		return errors.New("-socket PATH required; no positional arguments")
	}
	ctx := context.Background()
	if args[0] == "mcp" {
		return scheduler.ServeControlMCP(ctx, *socket, os.Stdin, os.Stdout)
	}
	var req scheduler.ControlRequest
	dec := json.NewDecoder(io.LimitReader(os.Stdin, scheduler.ControlMaxBytes+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return err
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("one JSON request required")
	}
	result, err := scheduler.CallControl(ctx, *socket, req)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

// schedTasks is heimdall sched tasks -config FILE -json: the running
// service's durable task queue (task_export) as one JSON object. It asks
// the service over its control socket and opens neither heimdall.db nor
// durable.db: DBOS runs only inside heimdall run.
func schedTasks(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("sched tasks", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print the task queue as JSON (required)")
	cfg, err := loadConfig(fs, args)
	if err != nil {
		return err
	}
	if !*asJSON || fs.NArg() != 0 {
		return errors.New("usage: heimdall sched tasks -config FILE -json")
	}
	if !cfg.Scheduler.On() || cfg.Scheduler.Control == nil {
		return errors.New("scheduler control is not configured: heimdall run serves no task export")
	}
	socket := filepath.Join(cfg.DataDir, "scheduler-control", "control.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := scheduler.CallControl(ctx, socket, scheduler.ControlRequest{Action: "task_export"})
	if err != nil && result.Error == "" {
		return fmt.Errorf("heimdall service not reachable: %s", oneLine(err.Error()))
	}
	if err != nil {
		return errors.New(oneLine(err.Error()))
	}
	var tasks struct {
		Tasks []durable.TaskRow `json:"tasks"`
	}
	dec := json.NewDecoder(strings.NewReader(result.Reply))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&tasks); err != nil || tasks.Tasks == nil || dec.Decode(new(any)) != io.EOF {
		return errors.New("heimdall service gave no task export")
	}
	_, err = fmt.Fprintln(out, string(bytes.TrimSpace([]byte(result.Reply))))
	return err
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }
