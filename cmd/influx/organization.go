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

// Organization Command
var organizationCmd = &cobra.Command{
	Use:     "org",
	Aliases: []string{"organization"},
	Short:   "Organization related commands",
	Run:     organizationF,
}

func organizationF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Create Command
type OrganizationCreateFlags struct {
	name string
}

var organizationCreateFlags OrganizationCreateFlags

func init() {
	organizationCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create organization",
		Run:   organizationCreateF,
	}

	organizationCreateCmd.Flags().StringVarP(&organizationCreateFlags.name, "name", "n", "", "name of organization that will be created")
	organizationCreateCmd.MarkFlagRequired("name")

	organizationCmd.AddCommand(organizationCreateCmd)
}

func organizationCreateF(cmd *cobra.Command, args []string) {
	orgS := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	o := &platform.Organization{
		Name: organizationCreateFlags.name,
	}

	if err := orgS.CreateOrganization(context.Background(), o); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   o.ID.String(),
		"Name": o.Name,
	})
	w.Flush()
}

// Find Command
type OrganizationFindFlags struct {
	name string
	id   string
}

var organizationFindFlags OrganizationFindFlags

func init() {
	organizationFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find organizations",
		Run:   organizationFindF,
	}

	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.name, "name", "n", "", "organization name")
	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.id, "id", "i", "", "organization id")

	organizationCmd.AddCommand(organizationFindCmd)
}

func organizationFindF(cmd *cobra.Command, args []string) {
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.OrganizationFilter{}
	if organizationFindFlags.name != "" {
		filter.Name = &organizationFindFlags.name
	}

	if organizationFindFlags.id != "" {
		filter.ID = &platform.ID{}
		if err := filter.ID.DecodeFromString(organizationFindFlags.id); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	orgs, _, err := s.FindOrganizations(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	for _, o := range orgs {
		w.Write(map[string]interface{}{
			"ID":   o.ID.String(),
			"Name": o.Name,
		})
	}
	w.Flush()
}

// Update Command
type OrganizationUpdateFlags struct {
	id   string
	name string
}

var organizationUpdateFlags OrganizationUpdateFlags

func init() {
	organizationUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update organization",
		Run:   organizationUpdateF,
	}

	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.id, "id", "i", "", "organization ID (required)")
	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.name, "name", "n", "", "organization name")
	organizationUpdateCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationUpdateCmd)
}

func organizationUpdateF(cmd *cobra.Command, args []string) {
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	err := id.DecodeFromString(organizationUpdateFlags.id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	update := platform.OrganizationUpdate{}
	if organizationUpdateFlags.name != "" {
		update.Name = &organizationUpdateFlags.name
	}

	o, err := s.UpdateOrganization(context.Background(), id, update)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   o.ID.String(),
		"Name": o.Name,
	})
	w.Flush()
}

// Delete command
type OrganizationDeleteFlags struct {
	id string
}

var organizationDeleteFlags OrganizationDeleteFlags

func organizationDeleteF(cmd *cobra.Command, args []string) {
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationDeleteFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	o, err := s.FindOrganizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = s.DeleteOrganization(ctx, id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":      o.ID.String(),
		"Name":    o.Name,
		"Deleted": true,
	})
	w.Flush()
}

func init() {
	organizationDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete organization",
		Run:   organizationDeleteF,
	}

	organizationDeleteCmd.Flags().StringVarP(&organizationDeleteFlags.id, "id", "i", "", "organization id (required)")
	organizationDeleteCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationDeleteCmd)
}

// Owner management
var organizationOwnersCmd = &cobra.Command{
	Use:   "owners",
	Short: "organization ownership commands",
	Run:   organizationF,
}

func init() {
	organizationCmd.AddCommand(organizationOwnersCmd)
}

// List Owners
type OrganizationOwnersListFlags struct {
	name string
	id   string
}

var organizationOwnersListFlags OrganizationOwnersListFlags

func organizationOwnersListF(cmd *cobra.Command, args []string) {
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	if organizationOwnersListFlags.id == "" && organizationOwnersListFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.OrganizationFilter{}
	if organizationOwnersListFlags.name != "" {
		filter.Name = &organizationOwnersListFlags.name
	}

	if organizationOwnersListFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationOwnersListFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := s.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	owners := organization.Owners

	// TODO: look up each user and output their name
	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
	)
	for _, owner := range owners {
		w.Write(map[string]interface{}{
			"ID": owner.ID.String(),
		})
	}
	w.Flush()
}

func init() {
	organizationOwnersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List organization owners",
		Run:   organizationOwnersListF,
	}

	organizationOwnersListCmd.Flags().StringVarP(&organizationOwnersListFlags.id, "id", "i", "", "organization id")
	organizationOwnersListCmd.Flags().StringVarP(&organizationOwnersListFlags.name, "name", "n", "", "organization name")

	organizationOwnersCmd.AddCommand(organizationOwnersListCmd)
}

// Add Owner
type OrganizationOwnersAddFlags struct {
	name    string
	id      string
	ownerId string
}

var organizationOwnersAddFlags OrganizationOwnersAddFlags

func organizationOwnersAddF(cmd *cobra.Command, args []string) {
	if organizationOwnersAddFlags.id == "" && organizationOwnersAddFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	if organizationOwnersAddFlags.id != "" && organizationOwnersAddFlags.name != "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.OrganizationFilter{}
	if organizationOwnersAddFlags.name != "" {
		filter.Name = &organizationOwnersListFlags.name
	}

	if organizationOwnersAddFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationOwnersAddFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := s.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ownerID := &platform.ID{}
	err = ownerID.DecodeFromString(organizationOwnersAddFlags.ownerId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	owner := &platform.Owner{ID: *ownerID}
	if err = s.AddOrganizationOwner(context.Background(), organization.ID, owner); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Owner added")
}

func init() {
	organizationOwnersAddCmd := &cobra.Command{
		Use:   "add",
		Short: "Add organization owner",
		Run:   organizationOwnersAddF,
	}

	organizationOwnersAddCmd.Flags().StringVarP(&organizationOwnersAddFlags.id, "id", "i", "", "organization id")
	organizationOwnersAddCmd.Flags().StringVarP(&organizationOwnersAddFlags.name, "name", "n", "", "organization name")
	organizationOwnersAddCmd.Flags().StringVarP(&organizationOwnersAddFlags.ownerId, "owner", "o", "", "owner id")
	organizationOwnersAddCmd.MarkFlagRequired("owner")

	organizationOwnersCmd.AddCommand(organizationOwnersAddCmd)
}

// Remove Owner
type OrganizationOwnersRemoveFlags struct {
	name    string
	id      string
	ownerId string
}

var organizationOwnersRemoveFlags OrganizationOwnersRemoveFlags

func organizationOwnersRemoveF(cmd *cobra.Command, args []string) {
	if organizationOwnersRemoveFlags.id == "" && organizationOwnersRemoveFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	if organizationOwnersRemoveFlags.id != "" && organizationOwnersRemoveFlags.name != "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.OrganizationFilter{}
	if organizationOwnersRemoveFlags.name != "" {
		filter.Name = &organizationOwnersRemoveFlags.name
	}

	if organizationOwnersRemoveFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationOwnersRemoveFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := s.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ownerID := &platform.ID{}
	err = ownerID.DecodeFromString(bucketOwnersRemoveFlags.ownerId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = s.RemoveOrganizationOwner(context.Background(), organization.ID, *ownerID); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Owner removed")
}

func init() {
	organizationOwnersRemoveCmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove organization owner",
		Run:   organizationOwnersRemoveF,
	}

	organizationOwnersRemoveCmd.Flags().StringVarP(&organizationOwnersRemoveFlags.id, "id", "i", "", "organization id")
	organizationOwnersRemoveCmd.Flags().StringVarP(&organizationOwnersRemoveFlags.name, "name", "n", "", "organization name")
	organizationOwnersRemoveCmd.Flags().StringVarP(&organizationOwnersRemoveFlags.ownerId, "owner", "o", "", "owner id")
	organizationOwnersRemoveCmd.MarkFlagRequired("owner")

	organizationOwnersCmd.AddCommand(organizationOwnersRemoveCmd)
}
