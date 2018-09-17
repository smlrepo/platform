package testing

import (
	"testing"

	"github.com/influxdata/platform"
)

// UserResourceFields includes prepopulated data for mapping tests
type UserResourceFields struct {
	UserResourceMappings []*platform.UserResourceMapping
}

type userResourceMappingServiceF func(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
)

// UserResourceMappingService tests all the service functions.
