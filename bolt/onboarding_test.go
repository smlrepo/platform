package bolt_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

func TestIsOnboarding(t *testing.T) {
	dir := os.TempDir()
	fName := dir + "/onboarding-test-db"
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	result := c.IsOnboarding()
	if result {
		t.Fatalf("isOnboarding testing fail, should be false")
	}
	defer closeFn()
	err = os.Remove(c.Path)
	if err != nil {
		t.Fatalf("isOnbaording testing fail, unable to remove temp file")
	}
	c.Path = fName
	result = c.IsOnboarding()
	if !result {
		t.Fatalf("isOnboarding testing fail, should be true")
	}
}

func TestGenerate(t *testing.T) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	defer closeFn()
	want := &platform.OnboardingDefaults{
		User: platform.User{
			ID:   idFromString(t, oneID),
			Name: "admin",
		},
		Org: platform.Organization{
			ID:   idFromString(t, twoID),
			Name: "default",
		},
		Bucket: platform.Bucket{
			ID:             idFromString(t, threeID),
			Name:           "default",
			Organization:   "default",
			OrganizationID: idFromString(t, twoID),
			Type:           platform.BucketTypeUser,
		},
		Auth: platform.Authorization{
			ID:     idFromString(t, fourID),
			Token:  oneToken,
			Status: platform.Active,
			User:   "admin",
			UserID: idFromString(t, oneID),
			Permissions: []platform.Permission{
				platform.CreateUserPermission,
				platform.DeleteUserPermission,
				platform.Permission{
					Resource: platform.OrganizationResource,
					Action:   platform.WriteAction,
				},
				platform.WriteBucketPermission(idFromString(t, threeID)),
			},
		},
	}
	c.IDGenerator = &loopIDGenerator{
		s: []string{oneID, twoID, threeID, fourID},
		t: t,
	}
	c.TokenGenerator = mock.NewTokenGenerator(oneToken, nil)
	result, err := c.Generate(context.TODO())
	if err != nil {
		t.Fatalf("onboarding generate failed: %v", err)
	}
	if diff := cmp.Diff(result, want); diff != "" {
		t.Errorf("onboarding defaults are different -got/+want\ndiff %s", diff)
	}
}

const (
	oneID    = "020f755c3c082000"
	twoID    = "020f755c3c082001"
	threeID  = "020f755c3c082002"
	fourID   = "020f755c3c082003"
	oneToken = "020f755c3c082008"
)

type loopIDGenerator struct {
	s []string
	p int
	t *testing.T
}

func (g *loopIDGenerator) ID() platform.ID {
	if g.p == len(g.s) {
		g.p = 0
	}
	id := idFromString(g.t, g.s[g.p])
	g.p++
	return id
}

func idFromString(t *testing.T, s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return *id
}
