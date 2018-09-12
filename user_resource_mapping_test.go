package platform_test

import (
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID platform.ID
		UserID     platform.ID
		UserType   platform.UserType
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "mapping requires a resourceid",
			fields: fields{
				UserID:   platformtesting.MustIDFromString("debac1e0deadbeef"),
				UserType: platform.Owner,
			},
			wantErr: true,
		},
		{
			name: "mapping requires an Owner",
			fields: fields{
				ResourceID: platformtesting.MustIDFromString("020f755c3c082000"),
				UserType:   platform.Owner,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a usertype",
			fields: fields{
				ResourceID: platformtesting.MustIDFromString("020f755c3c082000"),
				UserID:     platformtesting.MustIDFromString("debac1e0deadbeef"),
			},
			wantErr: true,
		},
		{
			name: "the usertype provided must be valid",
			fields: fields{
				ResourceID: platformtesting.MustIDFromString("020f755c3c082000"),
				UserID:     platformtesting.MustIDFromString("debac1e0deadbeef"),
				UserType:   "foo",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.UserResourceMapping{
				ResourceID: tt.fields.ResourceID,
				UserID:     tt.fields.UserID,
				UserType:   tt.fields.UserType,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
