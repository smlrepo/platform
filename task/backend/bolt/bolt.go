// Package bolt provides an bolt-backed store implementation.
//
// The data stored in bolt is structured as follows:
//
//    bucket(/tasks/v1/tasks) key(:task_id) -> Content of submitted task (i.e. flux code).
//    bucket(/tasks/v1/task_meta) key(:task_id) -> Protocol Buffer encoded backend.StoreTaskMeta,
//                                    so we have a consistent view of runs in progress and max concurrency.
//    bucket(/tasks/v1/org_by_task_id) key(task_id) -> The organization ID (stored as encoded string) associated with given task.
//    bucket(/tasks/v1/user_by_task_id) key(:task_id) -> The user ID (stored as encoded string) associated with given task.
//    buket(/tasks/v1/name_by_task_id) key(:task_id) -> The user-supplied name of the script.
//    bucket(/tasks/v1/run_ids) -> Counter for run IDs
//    bucket(/tasks/v1/orgs).bucket(:org_id) key(:task_id) -> Empty content; presence of :task_id allows for lookup from org to tasks.
//    bucket(/tasks/v1/users).bucket(:user_id) key(:task_id) -> Empty content; presence of :task_id allows for lookup from user to tasks.
// Note that task IDs are stored big-endian uint64s for sorting purposes,
// but presented to the users with leading 0-bytes stripped.
// Like other components of the system, IDs presented to users may be `0f12` rather than `f12`.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
	bolt "go.etcd.io/bbolt"
)

// ErrDBReadOnly is an error for when the database is set to read only.
// Tasks needs to be able to write to the db.
var ErrDBReadOnly = errors.New("db is read only")

// ErrMaxConcurrency is an error for when the max concurrency is already
// reached for a task when you try to schedule a task.
var ErrMaxConcurrency = errors.New("MaxConcurrency reached")

// ErrRunNotFound is an error for when a run isn't found in a FinishRun method.
var ErrRunNotFound = errors.New("run not found")

// ErrNotFound is an error for when a task could not be found
var ErrNotFound = errors.New("task not found")

// Store is task store for bolt.
type Store struct {
	db     *bolt.DB
	bucket []byte
}

const basePath = "/tasks/v1/"

var (
	tasksPath    = []byte(basePath + "tasks")
	orgsPath     = []byte(basePath + "orgs")
	usersPath    = []byte(basePath + "users")
	taskMetaPath = []byte(basePath + "task_meta")
	orgByTaskID  = []byte(basePath + "org_by_task_id")
	userByTaskID = []byte(basePath + "user_by_task_id")
	nameByTaskID = []byte(basePath + "name_by_task_id")
	runIDs       = []byte(basePath + "run_ids")
)

// New gives us a new Store based on "go.etcd.io/bbolt"
func New(db *bolt.DB, rootBucket string) (*Store, error) {
	if db.IsReadOnly() {
		return nil, ErrDBReadOnly
	}
	bucket := []byte(rootBucket)

	err := db.Update(func(tx *bolt.Tx) error {
		// create root
		root, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		// create the buckets inside the root
		for _, b := range [][]byte{
			tasksPath, orgsPath, usersPath, taskMetaPath,
			orgByTaskID, userByTaskID,
			nameByTaskID, runIDs,
		} {
			_, err := root.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Store{db: db, bucket: bucket}, nil
}

// CreateTask creates a task in the boltdb task store.
func (s *Store) CreateTask(ctx context.Context, req backend.CreateTaskRequest) (platform.ID, error) {
	o, err := backend.StoreValidator.CreateArgs(req)
	if err != nil {
		return platform.InvalidID(), err
	}

	var id platform.ID
	err = s.db.Update(func(tx *bolt.Tx) error {
		// get the root bucket
		b := tx.Bucket(s.bucket)
		name := []byte(o.Name)
		// Get ID
		idi, _ := b.NextSequence() // we ignore this err check, because this can't err inside an Update call
		id = platform.ID(idi)
		// Encode ID
		encodedID, err := id.Encode()
		if err != nil {
			return err
		}

		// write script
		err = b.Bucket(tasksPath).Put(encodedID, []byte(req.Script))
		if err != nil {
			return err
		}

		// name
		err = b.Bucket(nameByTaskID).Put(encodedID, name)
		if err != nil {
			return err
		}

		// Encode org ID
		encodedOrg, err := req.Org.Encode()
		if err != nil {
			return err
		}

		// org
		orgB, err := b.Bucket(orgsPath).CreateBucketIfNotExists(encodedOrg)
		if err != nil {
			return err
		}

		err = orgB.Put(encodedID, nil)
		if err != nil {
			return err
		}

		err = b.Bucket(orgByTaskID).Put(encodedID, encodedOrg)
		if err != nil {
			return err
		}

		// Encoded user ID
		encodedUser, err := req.User.Encode()
		if err != nil {
			return err
		}

		// user
		userB, err := b.Bucket(usersPath).CreateBucketIfNotExists(encodedUser)
		if err != nil {
			return err
		}

		err = userB.Put(encodedID, nil)
		if err != nil {
			return err
		}

		err = b.Bucket(userByTaskID).Put(encodedID, encodedUser)
		if err != nil {
			return err
		}

		stm := backend.StoreTaskMeta{
			MaxConcurrency:  int32(o.Concurrency),
			Status:          string(req.Status),
			LatestCompleted: req.ScheduleAfter,
			EffectiveCron:   o.EffectiveCronString(),
			Delay:           int32(o.Delay / time.Second),
		}
		if stm.Status == "" {
			stm.Status = string(backend.DefaultTaskStatus)
		}

		stmBytes, err := stm.Marshal()
		if err != nil {
			return err
		}
		metaB := b.Bucket(taskMetaPath)
		return metaB.Put(encodedID, stmBytes)
	})

	if err != nil {
		return platform.InvalidID(), err
	}

	return id, nil
}

// ModifyTask changes a task with a new script, it should error if the task does not exist.
func (s *Store) ModifyTask(ctx context.Context, id platform.ID, newScript string) error {
	op, err := backend.StoreValidator.ModifyArgs(id, newScript)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		bt := b.Bucket(tasksPath)

		encodedID, err := id.Encode()
		if err != nil {
			return err
		}

		if v := bt.Get(encodedID); v == nil {
			return backend.ErrTaskNotFound
		}
		err = bt.Put(encodedID, []byte(newScript))
		if err != nil {
			return err
		}
		return b.Bucket(nameByTaskID).Put(encodedID, []byte(op.Name))
	})
}

// ListTasks lists the tasks based on a filter.
func (s *Store) ListTasks(ctx context.Context, params backend.TaskSearchParams) ([]backend.StoreTask, error) {
	if params.Org.Valid() && params.User.Valid() {
		return nil, errors.New("ListTasks: org and user filters are mutually exclusive")
	}

	const (
		defaultPageSize = 100
		maxPageSize     = 500
	)
	if params.PageSize < 0 {
		return nil, errors.New("ListTasks: PageSize must be positive")
	}
	if params.PageSize > maxPageSize {
		return nil, fmt.Errorf("ListTasks: PageSize exceeds maximum of %d", maxPageSize)
	}
	lim := params.PageSize
	if lim == 0 {
		lim = defaultPageSize
	}
	taskIDs := make([]platform.ID, 0, params.PageSize)
	var tasks []backend.StoreTask

	if err := s.db.View(func(tx *bolt.Tx) error {
		var c *bolt.Cursor
		b := tx.Bucket(s.bucket)
		if params.Org.Valid() {
			encodedOrg, err := params.Org.Encode()
			if err != nil {
				return err
			}
			orgB := b.Bucket(orgsPath).Bucket(encodedOrg)
			if orgB == nil {
				return ErrNotFound
			}
			c = orgB.Cursor()
		} else if params.User.Valid() {
			encodedUser, err := params.User.Encode()
			if err != nil {
				return err
			}
			userB := b.Bucket(usersPath).Bucket(encodedUser)
			if userB == nil {
				return ErrNotFound
			}
			c = userB.Cursor()
		} else {
			c = b.Bucket(tasksPath).Cursor()
		}
		if params.After.Valid() {
			encodedAfter, err := params.After.Encode()
			if err != nil {
				return err
			}
			c.Seek(encodedAfter)
			for k, _ := c.Next(); k != nil && len(taskIDs) < lim; k, _ = c.Next() {
				var nID platform.ID
				if err := nID.Decode(k); err != nil {
					return err
				}
				taskIDs = append(taskIDs, nID)
			}
		} else {
			for k, _ := c.First(); k != nil && len(taskIDs) < lim; k, _ = c.Next() {
				var nID platform.ID
				if err := nID.Decode(k); err != nil {
					return err
				}
				taskIDs = append(taskIDs, nID)
			}
		}

		tasks = make([]backend.StoreTask, len(taskIDs))
		for i := range taskIDs {
			// TODO(docmerlin): optimization: don't check <-ctx.Done() every time though the loop
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// TODO(docmerlin): change the setup to reduce the number of lookups to 1 or 2.
				encodedID, err := taskIDs[i].Encode()
				if err != nil {
					return err
				}
				tasks[i].ID = taskIDs[i]
				tasks[i].Script = string(b.Bucket(tasksPath).Get(encodedID))
				tasks[i].Name = string(b.Bucket(nameByTaskID).Get(encodedID))
			}
		}
		if params.Org.Valid() {
			for i := range taskIDs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					encodedID, err := taskIDs[i].Encode()
					if err != nil {
						return err
					}
					tasks[i].Org = params.Org
					var userID platform.ID
					if err := userID.Decode(b.Bucket(userByTaskID).Get(encodedID)); err != nil {
						return err
					}
					tasks[i].User = userID
				}
			}
			return nil
		}
		if params.User.Valid() {
			for i := range taskIDs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					encodedID, err := taskIDs[i].Encode()
					if err != nil {
						return err
					}
					tasks[i].User = params.User
					var orgID platform.ID
					if err := orgID.Decode(b.Bucket(orgByTaskID).Get(encodedID)); err != nil {
						return err
					}
					tasks[i].Org = orgID
				}
			}
			return nil
		}
		for i := range taskIDs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				encodedID, err := taskIDs[i].Encode()
				if err != nil {
					return err
				}

				var userID platform.ID
				if err := userID.Decode(b.Bucket(userByTaskID).Get(encodedID)); err != nil {
					return err
				}
				tasks[i].User = userID

				var orgID platform.ID
				if err := orgID.Decode(b.Bucket(orgByTaskID).Get(encodedID)); err != nil {
					return err
				}
				tasks[i].Org = orgID
			}
		}
		return nil
	}); err != nil {
		if err == ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return tasks, nil
}

// FindTaskByID finds a task with a given an ID.  It will return nil if the task does not exist.
func (s *Store) FindTaskByID(ctx context.Context, id platform.ID) (*backend.StoreTask, error) {
	var userID, orgID platform.ID
	var script, name string
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		scriptBytes := b.Bucket(tasksPath).Get(encodedID)
		if scriptBytes == nil {
			return backend.ErrTaskNotFound
		}
		script = string(scriptBytes)

		if err := userID.Decode(b.Bucket(userByTaskID).Get(encodedID)); err != nil {
			return err
		}

		if err := orgID.Decode(b.Bucket(orgByTaskID).Get(encodedID)); err != nil {
			return err
		}

		name = string(b.Bucket(nameByTaskID).Get(encodedID))
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &backend.StoreTask{
		ID:     id,
		Org:    orgID,
		User:   userID,
		Name:   name,
		Script: script,
	}, err
}

func (s *Store) FindTaskMetaByID(ctx context.Context, id platform.ID) (*backend.StoreTaskMeta, error) {
	var stmBytes []byte
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes = b.Bucket(taskMetaPath).Get(encodedID)
		if stmBytes == nil {
			return backend.ErrTaskNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	stm := backend.StoreTaskMeta{}
	err = stm.Unmarshal(stmBytes)
	if err != nil {
		return nil, err
	}

	return &stm, nil
}

func (s *Store) FindTaskByIDWithMeta(ctx context.Context, id platform.ID) (*backend.StoreTask, *backend.StoreTaskMeta, error) {
	var stmBytes []byte
	var userID, orgID platform.ID
	var script, name string
	encodedID, err := id.Encode()
	if err != nil {
		return nil, nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		scriptBytes := b.Bucket(tasksPath).Get(encodedID)
		if scriptBytes == nil {
			return backend.ErrTaskNotFound
		}
		script = string(scriptBytes)

		// Assign copies of everything so we don't hold a stale reference to a bolt-maintained byte slice.
		stmBytes = append(stmBytes, b.Bucket(taskMetaPath).Get(encodedID)...)

		if err := userID.Decode(b.Bucket(userByTaskID).Get(encodedID)); err != nil {
			return err
		}

		if err := orgID.Decode(b.Bucket(orgByTaskID).Get(encodedID)); err != nil {
			return err
		}

		name = string(b.Bucket(nameByTaskID).Get(encodedID))
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	stm := backend.StoreTaskMeta{}
	if err := stm.Unmarshal(stmBytes); err != nil {
		return nil, nil, err
	}

	return &backend.StoreTask{
		ID:     id,
		Org:    orgID,
		User:   userID,
		Name:   name,
		Script: script,
	}, &stm, nil
}

func (s *Store) EnableTask(ctx context.Context, id platform.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket).Bucket(taskMetaPath)
		stmBytes := b.Get(encodedID)
		if stmBytes == nil {
			return errors.New("task meta not found")
		}
		stm := backend.StoreTaskMeta{}
		err := stm.Unmarshal(stmBytes)
		if err != nil {
			return err
		}
		stm.Status = string(backend.TaskActive)
		stmBytes, err = stm.Marshal()
		if err != nil {
			return err
		}

		return b.Put(encodedID, stmBytes)
	})
}

func (s *Store) DisableTask(ctx context.Context, id platform.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket).Bucket(taskMetaPath)
		stmBytes := b.Get(encodedID)
		if stmBytes == nil {
			return errors.New("task meta not found")
		}
		stm := backend.StoreTaskMeta{}
		err := stm.Unmarshal(stmBytes)
		if err != nil {
			return err
		}
		stm.Status = string(backend.TaskInactive)
		stmBytes, err = stm.Marshal()
		if err != nil {
			return err
		}

		return b.Put(encodedID, stmBytes)
	})
}

// DeleteTask deletes the task.
func (s *Store) DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error) {
	encodedID, err := id.Encode()
	if err != nil {
		return false, err
	}
	err = s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if check := b.Bucket(tasksPath).Get(encodedID); check == nil {
			return backend.ErrTaskNotFound
		}
		if err := b.Bucket(taskMetaPath).Delete(encodedID); err != nil {
			return err
		}
		if err := b.Bucket(tasksPath).Delete(encodedID); err != nil {
			return err
		}
		user := b.Bucket(userByTaskID).Get(encodedID)
		if len(user) > 0 {
			if err := b.Bucket(usersPath).Bucket(user).Delete(encodedID); err != nil {
				return err
			}
		}
		if err := b.Bucket(userByTaskID).Delete(encodedID); err != nil {
			return err
		}
		if err := b.Bucket(nameByTaskID).Delete(encodedID); err != nil {
			return err
		}

		org := b.Bucket(orgByTaskID).Get(encodedID)
		if len(org) > 0 {
			if err := b.Bucket(orgsPath).Bucket(org).Delete(encodedID); err != nil {
				return err
			}
		}
		return b.Bucket(orgByTaskID).Delete(encodedID)
	})
	if err != nil {
		if err == backend.ErrTaskNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) CreateNextRun(ctx context.Context, taskID platform.ID, now int64) (backend.RunCreation, error) {
	var rc backend.RunCreation

	encodedID, err := taskID.Encode()
	if err != nil {
		return rc, err
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes := b.Bucket(taskMetaPath).Get(encodedID)
		if stmBytes == nil {
			return backend.ErrTaskNotFound
		}

		var stm backend.StoreTaskMeta
		if err := stm.Unmarshal(stmBytes); err != nil {
			return err
		}

		makeID := func() (platform.ID, error) {
			idi, err := b.Bucket(runIDs).NextSequence()
			if err != nil {
				return platform.InvalidID(), err
			}

			return platform.ID(idi), nil
		}

		var err error
		rc, err = stm.CreateNextRun(now, makeID)
		if err != nil {
			return err
		}
		rc.Created.TaskID = taskID

		stmBytes, err = stm.Marshal()
		if err != nil {
			return err
		}
		return tx.Bucket(s.bucket).Bucket(taskMetaPath).Put(encodedID, stmBytes)
	}); err != nil {
		return backend.RunCreation{}, err
	}

	return rc, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *Store) FinishRun(ctx context.Context, taskID, runID platform.ID) error {
	encodedID, err := taskID.Encode()
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes := b.Bucket(taskMetaPath).Get(encodedID)
		var stm backend.StoreTaskMeta
		if err := stm.Unmarshal(stmBytes); err != nil {
			return err
		}
		if !stm.FinishRun(runID) {
			return ErrRunNotFound
		}

		stmBytes, err := stm.Marshal()
		if err != nil {
			return err
		}

		return tx.Bucket(s.bucket).Bucket(taskMetaPath).Put(encodedID, stmBytes)
	})
}

func (s *Store) ManuallyRunTimeRange(_ context.Context, taskID platform.ID, start, end, requestedAt int64) error {
	encodedID, err := taskID.Encode()
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes := b.Bucket(taskMetaPath).Get(encodedID)
		var stm backend.StoreTaskMeta
		if err := stm.Unmarshal(stmBytes); err != nil {
			return err
		}
		if err := stm.ManuallyRunTimeRange(start, end, requestedAt); err != nil {
			return err
		}

		stmBytes, err := stm.Marshal()
		if err != nil {
			return err
		}

		return tx.Bucket(s.bucket).Bucket(taskMetaPath).Put(encodedID, stmBytes)
	})
}

// Close closes the store
func (s *Store) Close() error {
	return s.db.Close()
}

// DeleteUser syncronously deletes a user and all their tasks from a bolt store.
func (s *Store) DeleteUser(ctx context.Context, id platform.ID) error {
	userID, err := id.Encode()
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		ub := b.Bucket(usersPath).Bucket(userID)
		if ub == nil {
			return backend.ErrUserNotFound
		}
		c := ub.Cursor()
		i := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
			// check for cancelation every 256 tasks deleted
			if i&0xFF == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			if err := b.Bucket(tasksPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(taskMetaPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(orgByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(userByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(nameByTaskID).Delete(k); err != nil {
				return err
			}

			org := b.Bucket(orgByTaskID).Get(k)
			if len(org) > 0 {
				ob := b.Bucket(orgsPath).Bucket(org)
				if ob != nil {
					if err := ob.Delete(k); err != nil {
						return err
					}
				}
			}
		}

		// check for cancelation one last time before we return
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return b.Bucket(usersPath).DeleteBucket(userID)
		}
	})

	return err
}

// DeleteOrg syncronously deletes an org and all their tasks from a bolt store.
func (s *Store) DeleteOrg(ctx context.Context, id platform.ID) error {
	orgID, err := id.Encode()
	if err != nil {
		return err
	}

	return s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		ob := b.Bucket(orgsPath).Bucket(orgID)
		if ob == nil {
			return backend.ErrOrgNotFound
		}
		c := ob.Cursor()
		i := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
			// check for cancelation every 256 tasks deleted
			if i&0xFF == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			if err := b.Bucket(tasksPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(taskMetaPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(orgByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(userByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(nameByTaskID).Delete(k); err != nil {
				return err
			}
			user := b.Bucket(userByTaskID).Get(k)
			if len(user) > 0 {
				ub := b.Bucket(usersPath).Bucket(user)
				if ub != nil {
					if err := ub.Delete(k); err != nil {
						return err
					}
				}
			}
		}
		// check for cancelation one last time before we return
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return b.Bucket(orgsPath).DeleteBucket(orgID)
		}
	})
}
