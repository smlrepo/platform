package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

const (
	orgOneID = "020f755c3c082000"
	orgTwoID = "020f755c3c082001"
)

var organizationCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Organization) []*platform.Organization {
		out := append([]*platform.Organization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// OrganizationFields will include the IDGenerator, and organizations
type OrganizationFields struct {
	IDGenerator   platform.IDGenerator
	Organizations []*platform.Organization
}

// CreateOrganization testing
func CreateOrganization(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		organization *platform.Organization
	}
	type wants struct {
		err           error
		organizations []*platform.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "create organizations with empty set",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgOneID, t),
				Organizations: []*platform.Organization{},
			},
			args: args{
				organization: &platform.Organization{
					Name: "name1",
				},
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						Name: "name1",
						ID:   MustIDFromString(orgOneID),
					},
				},
			},
		},
		{
			name: "basic create organization",
			fields: OrganizationFields{
				IDGenerator: mock.NewIDGenerator(orgTwoID, t),
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &platform.Organization{
					Name: "organization2",
				},
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "organization2",
					},
				},
			},
		},
		{
			name: "names should be unique",
			fields: OrganizationFields{
				IDGenerator: mock.NewIDGenerator(orgTwoID, t),
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &platform.Organization{
					Name: "organization1",
				},
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
				},
				err: fmt.Errorf("organization with name organization1 already exists"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.CreateOrganization(ctx, tt.args.organization)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			// Delete only newly created organizations
			// if tt.args.organization.ID != nil {
			defer s.DeleteOrganization(ctx, tt.args.organization.ID)
			// }

			organizations, _, err := s.FindOrganizations(ctx, platform.OrganizationFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve organizations: %v", err)
			}
			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganizationByID testing
func FindOrganizationByID(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err          error
		organization *platform.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic find organization by id",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id: MustIDFromString(orgTwoID),
			},
			wants: wants{
				organization: &platform.Organization{
					ID:   MustIDFromString(orgTwoID),
					Name: "organization2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			organization, err := s.FindOrganizationByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganizations testing
func FindOrganizations(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		ID   platform.ID
		name string
	}

	type wants struct {
		organizations []*platform.Organization
		err           error
	}
	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "find all organizations",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find organization by id",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				ID: MustIDFromString(orgTwoID),
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find organization by name",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			filter := platform.OrganizationFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			organizations, _, err := s.FindOrganizations(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteOrganization testing
func DeleteOrganization(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err           error
		organizations []*platform.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "delete organizations using exist id",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						Name: "orgA",
						ID:   MustIDFromString(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDFromString(orgTwoID),
					},
				},
			},
			args: args{
				ID: orgOneID,
			},
			wants: wants{
				organizations: []*platform.Organization{
					{
						Name: "orgB",
						ID:   MustIDFromString(orgTwoID),
					},
				},
			},
		},
		{
			name: "delete organizations using id that does not exist",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						Name: "orgA",
						ID:   MustIDFromString(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDFromString(orgTwoID),
					},
				},
			},
			args: args{
				ID: "1234567890654321",
			},
			wants: wants{
				err: fmt.Errorf("organization not found"),
				organizations: []*platform.Organization{
					{
						Name: "orgA",
						ID:   MustIDFromString(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDFromString(orgTwoID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteOrganization(ctx, MustIDFromString(tt.args.ID))
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.OrganizationFilter{}
			organizations, _, err := s.FindOrganizations(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve organizations: %v", err)
			}
			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganization testing
func FindOrganization(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		name string
	}

	type wants struct {
		organization *platform.Organization
		err          error
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "find organization by name",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				organization: &platform.Organization{
					ID:   MustIDFromString(orgOneID),
					Name: "abc",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			filter := platform.OrganizationFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			organization, err := s.FindOrganization(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateOrganization testing
func UpdateOrganization(
	init func(OrganizationFields, *testing.T) (platform.OrganizationService, func()),
	t *testing.T,
) {
	type args struct {
		name string
		id   platform.ID
	}
	type wants struct {
		err          error
		organization *platform.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: OrganizationFields{
				Organizations: []*platform.Organization{
					{
						ID:   MustIDFromString(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDFromString(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDFromString(orgOneID),
				name: "changed",
			},
			wants: wants{
				organization: &platform.Organization{
					ID:   MustIDFromString(orgOneID),
					Name: "changed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			upd := platform.OrganizationUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			organization, err := s.UpdateOrganization(ctx, tt.args.id, upd)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
