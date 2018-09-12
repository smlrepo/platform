package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	platformtesting "github.com/influxdata/platform/testing"
	"github.com/julienschmidt/httprouter"
)

func TestService_handleGetBuckets(t *testing.T) {
	type fields struct {
		BucketService platform.BucketService
	}
	type args struct {
		queryParams map[string][]string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get all buckets",
			fields: fields{
				&mock.BucketService{
					FindBucketsFn: func(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
						return []*platform.Bucket{
							{
								ID:             platformtesting.MustIDFromString("0b501e7e557ab1ed"),
								Name:           "hello",
								OrganizationID: platformtesting.MustIDFromString("50f7ba1150f7ba11"),
							},
							{
								ID:             platformtesting.MustIDFromString("c0175f0077a77005"),
								Name:           "example",
								OrganizationID: platformtesting.MustIDFromString("7e55e118dbabb1ed"),
							},
						}, 2, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/v1/buckets"
  },
  "buckets": [
    {
      "links": {
        "org": "/v1/orgs/50f7ba1150f7ba11",
        "self": "/v1/buckets/0b501e7e557ab1ed"
      },
      "id": "0b501e7e557ab1ed",
      "organizationID": "50f7ba1150f7ba11",
      "name": "hello",
      "retentionPeriod": 0
    },
    {
      "links": {
        "org": "/v1/orgs/7e55e118dbabb1ed",
        "self": "/v1/buckets/c0175f0077a77005"
      },
      "id": "c0175f0077a77005",
      "organizationID": "7e55e118dbabb1ed",
      "name": "example",
      "retentionPeriod": 0
    }
  ]
}
`,
			},
		},
		{
			name: "get all buckets when there are none",
			fields: fields{
				&mock.BucketService{
					FindBucketsFn: func(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
						return []*platform.Bucket{}, 0, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/v1/buckets"
  },
  "buckets": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewBucketHandler()
			h.BucketService = tt.fields.BucketService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetBuckets(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetBuckets() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetBuckets() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetBuckets() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}

		})
	}
}

func TestService_handleGetBucket(t *testing.T) {
	type fields struct {
		BucketService platform.BucketService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a bucket by id",
			fields: fields{
				&mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
						if id == platformtesting.MustIDFromString("020f755c3c082000") {
							return &platform.Bucket{
								ID:             platformtesting.MustIDFromString("020f755c3c082000"),
								OrganizationID: platformtesting.MustIDFromString("020f755c3c082000"),
								Name:           "hello",
							}, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "org": "/v1/orgs/020f755c3c082000",
    "self": "/v1/buckets/020f755c3c082000"
  },
  "id": "020f755c3c082000",
  "organizationID": "020f755c3c082000",
  "name": "hello",
  "retentionPeriod": 0
}
`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
						return nil, fmt.Errorf("bucket not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewBucketHandler()
			h.BucketService = tt.fields.BucketService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetBucket(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)
			t.Logf(res.Header.Get("X-Influx-Error"))

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetBucket() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetBucket() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetBucket() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handlePostBucket(t *testing.T) {
	type fields struct {
		BucketService platform.BucketService
	}
	type args struct {
		bucket *platform.Bucket
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "create a new bucket",
			fields: fields{
				&mock.BucketService{
					CreateBucketFn: func(ctx context.Context, c *platform.Bucket) error {
						c.ID = platformtesting.MustIDFromString("020f755c3c082000")
						return nil
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "hello",
					OrganizationID: platformtesting.MustIDFromString("6f626f7274697320"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "org": "/v1/orgs/6f626f7274697320",
    "self": "/v1/buckets/020f755c3c082000"
  },
  "id": "020f755c3c082000",
  "organizationID": "6f626f7274697320",
  "organization": "30",
  "name": "hello",
  "retentionPeriod": 0
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewBucketHandler()
			h.BucketService = tt.fields.BucketService

			b, err := json.Marshal(tt.args.bucket)
			if err != nil {
				t.Fatalf("failed to unmarshal bucket: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url?org=30", bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.handlePostBucket(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostBucket() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostBucket() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostBucket() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handleDeleteBucket(t *testing.T) {
	type fields struct {
		BucketService platform.BucketService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a bucket by id",
			fields: fields{
				&mock.BucketService{
					DeleteBucketFn: func(ctx context.Context, id platform.ID) error {
						if id == platformtesting.MustIDFromString("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "bucket not found",
			fields: fields{
				&mock.BucketService{
					DeleteBucketFn: func(ctx context.Context, id platform.ID) error {
						return fmt.Errorf("bucket not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewBucketHandler()
			h.BucketService = tt.fields.BucketService

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteBucket(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteBucket() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteBucket() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleDeleteBucket() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestService_handlePatchBucket(t *testing.T) {
	type fields struct {
		BucketService platform.BucketService
	}
	type args struct {
		id        string
		name      string
		retention time.Duration
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update a bucket name and retenion",
			fields: fields{
				&mock.BucketService{
					UpdateBucketFn: func(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
						if id == platformtesting.MustIDFromString("020f755c3c082000") {
							d := &platform.Bucket{
								ID:             platformtesting.MustIDFromString("020f755c3c082000"),
								Name:           "hello",
								OrganizationID: platformtesting.MustIDFromString("020f755c3c082000"),
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							if upd.RetentionPeriod != nil {
								d.RetentionPeriod = *upd.RetentionPeriod
							}

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:        "020f755c3c082000",
				name:      "example",
				retention: 1234,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "org": "/v1/orgs/020f755c3c082000",
    "self": "/v1/buckets/020f755c3c082000"
  },
  "id": "020f755c3c082000",
  "organizationID": "020f755c3c082000",
  "name": "example",
  "retentionPeriod": 1234
}
`,
			},
		},
		{
			name: "bucket not found",
			fields: fields{
				&mock.BucketService{
					UpdateBucketFn: func(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				name: "hello",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewBucketHandler()
			h.BucketService = tt.fields.BucketService

			upd := platform.BucketUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.retention != 0 {
				upd.RetentionPeriod = &tt.args.retention
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal bucket update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePatchBucket(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchBucket() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchBucket() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePatchBucket() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
