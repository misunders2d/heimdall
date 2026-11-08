package router

import (
	"errors"
	"github.com/misunders2d/heimdall/internal/harness"
	"github.com/misunders2d/heimdall/internal/scheduler"
)

func (r *Router) schedulerCaller(pid int) (scheduler.Caller, func() bool, error) {
	ancestors := harness.ProcessAncestors(pid)
	r.runsMu.Lock()
	defer r.runsMu.Unlock()
	for run := range r.runs {
		if c, generation := run.ControlGeneration(ancestors); generation != nil {
			job, id := generation.ControlScheduled()
			return scheduler.Caller{Principal: c.Principal, Role: c.Role, Conv: c.Conv, Private: c.Private, Job: job, Run: id, OAuthActor: c.OAuthActor, OAuthInstallation: c.OAuthInstallation}, func() bool {
				_, current := run.ControlGeneration(harness.ProcessAncestors(pid))
				return current == generation
			}, nil
		}
	}
	return scheduler.Caller{}, nil, errors.New("no active person turn for scheduler peer")
}
