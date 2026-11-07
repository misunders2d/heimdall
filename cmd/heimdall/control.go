package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"

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
