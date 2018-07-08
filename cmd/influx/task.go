package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
)

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "task related commands",
	Run:   taskF,
}

func taskF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Create Command
type TaskCreateFlags struct {
	name string
	flux string
}

var taskCreateFlags TaskCreateFlags

func init() {
	taskCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create task",
		Run:   taskCreateF,
	}

	taskCreateCmd.Flags().StringVarP(&taskCreateFlags.name, "name", "n", "", "task name")
	taskCreateCmd.Flags().StringVarP(&taskCreateFlags.flux, "flux", "f", "", "flux to create")

	taskCmd.AddCommand(taskCreateCmd)
}

func taskCreateF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	t := &platform.Task{
		Name: taskCreateFlags.name,
		Flux: taskCreateFlags.flux,
	}

	if err := s.CreateTask(context.Background(), t); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Find Command
// TODO: add filter by owner
type TaskFindFlags struct {
	id    string
	orgID string
}

var taskFindFlags TaskFindFlags

func init() {
	taskFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find tasks",
		Run:   taskFindF,
	}

	taskFindCmd.Flags().StringVarP(&taskFindFlags.id, "id", "i", "", "task ID")
	taskFindCmd.Flags().StringVarP(&taskFindFlags.orgID, "org-id", "", "", "task organization ID")

	taskCmd.AddCommand(taskFindCmd)
}

func taskFindF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.TaskFilter{}
	if taskFindFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(taskFindFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if taskFindFlags.orgID != "" {
		filter.Organization = &platform.ID{}
		err := filter.Organization.DecodeFromString(taskFindFlags.orgID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	tasks, _, err := s.FindTasks(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Flux",
		"Status",
	)
	for _, task := range tasks {
		w.Write(map[string]interface{}{
			"ID":     task.ID.String(),
			"Name":   task.Name,
			"Flux":   task.Flux,
			"Status": task.Status,
		})
	}
	w.Flush()
}

// Update Command
type TaskUpdateFlags struct {
	flux string
	id   string
}

var taskUpdateFlags TaskUpdateFlags

func init() {
	taskUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update task",
		Run:   taskUpdateF,
	}

	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.id, "id", "i", "", "task ID")
	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.flux, "flux", "f", "", "flux command")

	taskCmd.AddCommand(taskUpdateCmd)
}

func taskUpdateF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	if err := id.DecodeFromString(taskUpdateFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	update := platform.TaskUpdate{}
	if taskUpdateFlags.flux != "" {
		update.Flux = &taskUpdateFlags.flux
	}

	task, err := s.UpdateTask(context.Background(), id, update)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Flux",
		"Status",
	)
	w.Write(map[string]interface{}{
		"ID":     task.ID.String(),
		"Name":   task.Name,
		"Flux":   task.Flux,
		"Status": task.Status,
	})
	w.Flush()
}

// Delete command
type TaskDeleteFlags struct {
	id string
}

var taskDeleteFlags TaskDeleteFlags

func init() {
	taskDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete task",
		Run:   taskDeleteF,
	}

	taskDeleteCmd.Flags().StringVarP(&taskFindFlags.id, "id", "i", "", "task ID")

	taskCmd.AddCommand(taskDeleteCmd)
}

func taskDeleteF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	err := id.DecodeFromString(taskDeleteFlags.id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	task, err := s.FindTaskByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = s.DeleteTask(ctx, id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Flux",
		"Status",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":      task.ID.String(),
		"Name":    task.Name,
		"Flux":    task.Flux,
		"Status":  task.Status,
		"Deleted": true,
	})
	w.Flush()
}

// Owner management
var taskOwnersCmd = &cobra.Command{
	Use:   "owners",
	Short: "task ownership commands",
	Run:   taskF,
}

func init() {
	taskCmd.AddCommand(taskOwnersCmd)
}

// List Owners
type TaskOwnersListFlags struct {
	name string
	id   string
}

var taskOwnersListFlags TaskOwnersListFlags

func taskOwnersListF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if taskOwnersListFlags.id == "" && taskOwnersListFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.TaskFilter{}
	if taskOwnersListFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(taskOwnersListFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	task, err := s.FindTask(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	owners := task.Owners

	// TODO: look up each user and output their name
	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
	)
	for _, id := range owners {
		w.Write(map[string]interface{}{
			"ID": id.String(),
		})
	}
	w.Flush()
}

func init() {
	taskOwnersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List task owners",
		Run:   taskOwnersListF,
	}

	taskOwnersListCmd.Flags().StringVarP(&taskOwnersListFlags.id, "id", "i", "", "task id")
	taskOwnersListCmd.Flags().StringVarP(&taskOwnersListFlags.name, "name", "n", "", "task name")

	taskOwnersCmd.AddCommand(taskOwnersListCmd)
}

// Add Owner
type TaskOwnersAddFlags struct {
	name    string
	id      string
	ownerId string
}

var taskOwnersAddFlags TaskOwnersAddFlags

func taskOwnersAddF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if taskOwnersAddFlags.id == "" && taskOwnersAddFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.TaskFilter{}
	if taskOwnersAddFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(taskOwnersAddFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	task, err := s.FindTask(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var upd platform.TaskUpdate
	owners := task.Owners

	ownerExists := false
	for _, owner := range owners {
		if owner.String() != taskOwnersAddFlags.ownerId {
			ownerExists = true
			break
		}
	}

	if ownerExists {
		id := &platform.ID{}
		err := id.DecodeFromString(taskOwnersAddFlags.ownerId)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		owners = append(owners, *id)
		upd.Owners = &owners

		_, err = s.UpdateTask(context.Background(), task.ID, upd)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// TODO: look up each user and output their name
	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
	)
	for _, id := range owners {
		w.Write(map[string]interface{}{
			"ID": id.String(),
		})
	}
	w.Flush()
}

func init() {
	taskOwnersAddCmd := &cobra.Command{
		Use:   "add",
		Short: "Add task owner",
		Run:   taskOwnersAddF,
	}

	taskOwnersAddCmd.Flags().StringVarP(&taskOwnersAddFlags.id, "id", "i", "", "task id")
	taskOwnersAddCmd.Flags().StringVarP(&taskOwnersAddFlags.name, "name", "n", "", "task name")
	taskOwnersAddCmd.Flags().StringVarP(&taskOwnersAddFlags.ownerId, "owner", "o", "", "owner id")
	taskOwnersAddCmd.MarkFlagRequired("owner")

	taskOwnersCmd.AddCommand(taskOwnersAddCmd)
}

// Remove Owner
type TaskOwnersRemoveFlags struct {
	name    string
	id      string
	ownerId string
}

var taskOwnersRemoveFlags TaskOwnersRemoveFlags

func taskOwnersRemoveF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if taskOwnersRemoveFlags.id == "" && taskOwnersRemoveFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.TaskFilter{}
	if taskOwnersRemoveFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(taskOwnersRemoveFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	task, err := s.FindTask(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var upd platform.TaskUpdate
	owners := task.Owners

	for i, owner := range owners {
		if owner.String() == taskOwnersRemoveFlags.ownerId {
			updatedOwners := append(owners[:i], owners[i+1:]...)
			upd.Owners = &updatedOwners
			_, err = s.UpdateTask(context.Background(), task.ID, upd)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// TODO: look up each user and output their name
			w := internal.NewTabWriter(os.Stdout)
			w.WriteHeaders(
				"ID",
			)
			for _, id := range updatedOwners {
				w.Write(map[string]interface{}{
					"ID": id.String(),
				})
			}
			w.Flush()

			break
		}
	}
}

func init() {
	taskOwnersRemoveCmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove task owner",
		Run:   taskOwnersRemoveF,
	}

	taskOwnersRemoveCmd.Flags().StringVarP(&taskOwnersRemoveFlags.id, "id", "i", "", "task id")
	taskOwnersRemoveCmd.Flags().StringVarP(&taskOwnersRemoveFlags.name, "name", "n", "", "task name")
	taskOwnersRemoveCmd.Flags().StringVarP(&taskOwnersRemoveFlags.ownerId, "owner", "o", "", "owner id")
	taskOwnersRemoveCmd.MarkFlagRequired("owner")

	taskOwnersCmd.AddCommand(taskOwnersRemoveCmd)
}
