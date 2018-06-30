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
	taskCreateCmd.Flags().StringVarP(&taskCreateFlags.flux, "flux", "f", "", "ifql to execute")

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

	updateRequired := false
	for _, owner := range owners {
		if owner.String() == taskOwnersAddFlags.ownerId {
			updateRequired = true
			break
		}
	}

	if updateRequired {
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

// Delete Owner
type TaskOwnersDeleteFlags struct {
	name    string
	id      string
	ownerId string
}

var taskOwnersDeleteFlags TaskOwnersDeleteFlags

func taskOwnersDeleteF(cmd *cobra.Command, args []string) {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if taskOwnersDeleteFlags.id == "" && taskOwnersDeleteFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.TaskFilter{}
	if taskOwnersDeleteFlags.name != "" {
		filter.Name = &taskOwnersDeleteFlags.name
	}

	if taskOwnersDeleteFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(taskOwnersDeleteFlags.id)
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
		if owner.String() == taskOwnersDeleteFlags.ownerId {
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
	taskOwnersDeleteCmd := &cobra.Command{
		Use:   "remove",
		Short: "Delete task owner",
		Run:   taskOwnersDeleteF,
	}

	taskOwnersDeleteCmd.Flags().StringVarP(&taskOwnersDeleteFlags.id, "id", "i", "", "task id")
	taskOwnersDeleteCmd.Flags().StringVarP(&taskOwnersDeleteFlags.name, "name", "n", "", "task name")
	taskOwnersDeleteCmd.Flags().StringVarP(&taskOwnersDeleteFlags.ownerId, "owner", "o", "", "owner id")
	taskOwnersDeleteCmd.MarkFlagRequired("owner")

	taskOwnersCmd.AddCommand(taskOwnersDeleteCmd)
}
