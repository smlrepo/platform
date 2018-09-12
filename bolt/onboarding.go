package bolt

import (
	"context"
	"os"

	"github.com/influxdata/platform"
)

var _ platform.OnboardingService = (*Client)(nil)

// IsOnboarding checks the bolt path
// and determine if it is onboarding.
func (c *Client) IsOnboarding() bool {
	if _, err := os.Stat(c.Path); os.IsNotExist(err) {
		return true
	}
	return false
}

// Generate OnboardingDefaults.
func (c *Client) Generate(ctx context.Context) (*platform.OnboardingDefaults, error) {
	u := &platform.User{Name: "admin"}
	err := c.CreateUser(ctx, u)
	if err != nil {
		return nil, err
	}
	o := &platform.Organization{
		Name: "default",
	}
	err = c.CreateOrganization(ctx, o)
	if err != nil {
		return nil, err
	}
	bucket := &platform.Bucket{
		Name:           "default",
		Organization:   o.Name,
		OrganizationID: o.ID,
	}
	err = c.CreateBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	auth := &platform.Authorization{
		User:   u.Name,
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.CreateUserPermission,
			platform.DeleteUserPermission,
			platform.Permission{
				Resource: platform.OrganizationResource,
				Action:   platform.WriteAction,
			},
			platform.WriteBucketPermission(bucket.ID),
		},
	}
	err = c.CreateAuthorization(ctx, auth)
	if err != nil {
		return nil, err
	}
	return &platform.OnboardingDefaults{
		User:   *u,
		Org:    *o,
		Bucket: *bucket,
		Auth:   *auth,
	}, nil
}
