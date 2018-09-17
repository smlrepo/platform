package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initUserResourceMappingService(f platformtesting.UserResourceFields, t *testing.T) (platform.UserResourceMappingService, func()) {
	s := NewService()
	ctx := context.TODO()
	for _, m := range f.UserResourceMappings {
		if err := s.PutUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate mappingsn")
		}
	}

	return s, func() {}
}

func TestUserResourceMappingService_FindUserResourceMapping(t *testing.T) {
	platformtesting.FindUserResourceMapping(initUserResourceMappingService, t)
}

func TestUserResourceMappingService_CreateUserResourceMapping(t *testing.T) {
	platformtesting.CreateUserResourceMapping(initUserResourceMappingService, t)
}

func TestUserResourceMappingService_DeleteUserResourceMapping(t *testing.T) {
	platformtesting.DeleteUserResourceMapping(initUserResourceMappingService, t)
}
