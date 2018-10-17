package backend

// The tooling needed to correctly run go generate is managed by the Makefile.
// Run `make` from the project root to ensure these generate commands execute correctly.
//go:generate protoc -I ../../internal -I . --plugin ../../scripts/protoc-gen-gogofaster --gogofaster_out=plugins=grpc:. ./meta.proto

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/options"
)

var (
	// ErrTaskNotFound indicates no task could be found for given parameters.
	ErrTaskNotFound = errors.New("task not found")

	// ErrUserNotFound is an error for when we can't find a user
	ErrUserNotFound = errors.New("user not found")

	// ErrOrgNotFound is an error for when we can't find an org
	ErrOrgNotFound = errors.New("org not found")

	// ErrManualQueueFull is returned when a manual run request cannot be completed.
	ErrManualQueueFull = errors.New("manual queue at capacity")
)

type TaskStatus string

const (
	TaskActive   TaskStatus = "active"
	TaskInactive TaskStatus = "inactive"

	DefaultTaskStatus TaskStatus = TaskActive
)

type RunStatus int

const (
	RunStarted RunStatus = iota
	RunSuccess
	RunFail
	RunCanceled
)

func (r RunStatus) String() string {
	switch r {
	case RunStarted:
		return "started"
	case RunSuccess:
		return "success"
	case RunFail:
		return "failed"
	case RunCanceled:
		return "canceled"
	}
	panic(fmt.Sprintf("unknown RunStatus: %d", r))
}

// RunNotYetDueError is returned from CreateNextRun if a run is not yet due.
type RunNotYetDueError struct {
	// DueAt is the unix timestamp of when the next run is due.
	DueAt int64
}

func (e RunNotYetDueError) Error() string {
	return "run not due until " + time.Unix(e.DueAt, 0).UTC().Format(time.RFC3339)
}

// RunCreation is returned by CreateNextRun.
type RunCreation struct {
	Created QueuedRun

	// Unix timestamp for when the next run is due.
	NextDue int64

	// Whether there are any manual runs queued for this task.
	// If so, the scheduler should begin executing them after handling real-time tasks.
	HasQueue bool
}

type CreateTaskRequest struct {
	// Owners.
	Org, User platform.ID

	// Script content of the task.
	Script string

	// Unix timestamp (seconds elapsed since January 1, 1970 UTC).
	// The first run of the task will be run according to the earliest time after ScheduleAfter,
	// matching the task's schedul via its cron or every option.
	ScheduleAfter int64

	// The initial task status.
	// If empty, will be treated as DefaultTaskStatus.
	Status TaskStatus
}

// Store is the interface around persisted tasks.
type Store interface {
	// CreateTask creates a task with from the given CreateTaskRequest.
	// If the task is created successfully, the ID of the new task is returned.
	CreateTask(ctx context.Context, req CreateTaskRequest) (platform.ID, error)

	// ModifyTask updates the script of an existing task.
	// It returns an error if there was no task matching the given ID.
	ModifyTask(ctx context.Context, id platform.ID, newScript string) error

	// ListTasks lists the tasks in the store that match the search params.
	ListTasks(ctx context.Context, params TaskSearchParams) ([]StoreTask, error)

	// FindTaskByID returns the task with the given ID.
	// If no task matches the ID, the returned task is nil.
	FindTaskByID(ctx context.Context, id platform.ID) (*StoreTask, error)

	// FindTaskMetaByID returns the metadata about a task.
	FindTaskMetaByID(ctx context.Context, id platform.ID) (*StoreTaskMeta, error)

	// FindTaskByIDWithMeta combines finding the task and the meta into a single call.
	FindTaskByIDWithMeta(ctx context.Context, id platform.ID) (*StoreTask, *StoreTaskMeta, error)

	// EnableTask updates task status to active.
	EnableTask(ctx context.Context, id platform.ID) error

	// DisableTask updates task status to inactive.
	DisableTask(ctx context.Context, id platform.ID) error

	// DeleteTask returns whether an entry matching the given ID was deleted.
	// If err is non-nil, deleted is false.
	// If err is nil, deleted is false if no entry matched the ID,
	// or deleted is true if there was a matching entry and it was deleted.
	DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error)

	// CreateNextRun creates the earliest needed run scheduled no later than the given Unix timestamp now.
	// Internally, the Store should rely on the underlying task's StoreTaskMeta to create the next run.
	CreateNextRun(ctx context.Context, taskID platform.ID, now int64) (RunCreation, error)

	// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID platform.ID) error

	// ManuallyRunTimeRange enqueues a request to run the task with the given ID for all schedules no earlier than start and no later than end (Unix timestamps).
	// requestedAt is the Unix timestamp when the request was initiated.
	// ManuallyRunTimeRange must delegate to an underlying StoreTaskMeta's ManuallyRunTimeRange method.
	ManuallyRunTimeRange(ctx context.Context, taskID platform.ID, start, end, requestedAt int64) error

	// DeleteOrg deletes the org.
	DeleteOrg(ctx context.Context, orgID platform.ID) error

	// DeleteUser deletes a user with userID.
	DeleteUser(ctx context.Context, userID platform.ID) error

	// Close closes the store for usage and cleans up running processes.
	Close() error
}

// RunLogBase is the base information for a logs about an individual run.
type RunLogBase struct {
	// The parent task that owns the run.
	Task *StoreTask

	// The ID of the run.
	RunID platform.ID

	// The Unix timestamp indicating the run's scheduled time.
	RunScheduledFor int64

	// When the log is requested, should be ignored when it is zero.
	RequestedAt int64
}

// LogWriter writes task logs and task state changes to a store.
type LogWriter interface {
	// UpdateRunState sets the run state and the respective time.
	UpdateRunState(ctx context.Context, base RunLogBase, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, base RunLogBase, when time.Time, log string) error
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogWriter struct{}

func (NopLogWriter) UpdateRunState(context.Context, RunLogBase, time.Time, RunStatus) error {
	return nil
}

func (NopLogWriter) AddRunLog(context.Context, RunLogBase, time.Time, string) error {
	return nil
}

// LogReader reads log information and log data from a store.
type LogReader interface {
	// ListRuns returns a list of runs belonging to a task.
	ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error)

	// FindRunByID finds a run given a orgID and runID.
	FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error)

	// ListLogs lists logs for a task or a specified run of a task.
	ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error)
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogReader struct{}

func (NopLogReader) ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error) {
	return nil, nil
}

func (NopLogReader) FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error) {
	return nil, nil
}

func (NopLogReader) ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error) {
	return nil, nil
}

// TaskSearchParams is used when searching or listing tasks.
type TaskSearchParams struct {
	// Return tasks belonging to this exact organization ID. May be nil.
	Org platform.ID

	// Return tasks belonging to this exact user ID. May be nil.
	User platform.ID

	// Return tasks starting after this ID.
	After platform.ID

	// Size of each page. Must be non-negative.
	// If zero, the implementation picks an appropriate default page size.
	// Valid page sizes are implementation-dependent.
	PageSize int
}

// StoreTask is a stored representation of a Task.
type StoreTask struct {
	ID platform.ID

	// IDs for the owning organization and user.
	Org, User platform.ID

	// The user-supplied name of the Task.
	Name string

	// The script content of the task.
	Script string
}

// StoreValidator is a package-level StoreValidation, so that you can write
//    backend.StoreValidator.CreateArgs(...)
var StoreValidator StoreValidation

// StoreValidation is used for namespacing the store validation methods.
type StoreValidation struct{}

// CreateArgs returns the script's parsed options,
// and an error if any of the provided fields are invalid for creating a task.
func (StoreValidation) CreateArgs(req CreateTaskRequest) (options.Options, error) {
	var missing []string
	var o options.Options

	if req.Script == "" {
		missing = append(missing, "script")
	} else {
		var err error
		o, err = options.FromScript(req.Script)
		if err != nil {
			return o, err
		}
	}

	if !req.Org.Valid() {
		missing = append(missing, "organization ID")
	}
	if !req.User.Valid() {
		missing = append(missing, "user ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to create task: %s", strings.Join(missing, ", "))
	}

	if req.Status != "" && req.Status != TaskActive && req.Status != TaskInactive {
		return o, fmt.Errorf("invalid status: %s", req.Status)
	}

	return o, nil
}

// ModifyArgs returns the script's parsed options,
// and an error if any of the provided fields are invalid for modifying a task.
func (StoreValidation) ModifyArgs(taskID platform.ID, script string) (options.Options, error) {
	var missing []string
	var o options.Options

	if script == "" {
		missing = append(missing, "script")
	} else {
		var err error
		o, err = options.FromScript(script)
		if err != nil {
			return o, err
		}
	}

	if !taskID.Valid() {
		missing = append(missing, "task ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to modify task: %s", strings.Join(missing, ", "))
	}

	return o, nil
}
