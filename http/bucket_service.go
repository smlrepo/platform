package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/influxdata/platform"
	errors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// BucketHandler represents an HTTP API handler for buckets.
type BucketHandler struct {
	*httprouter.Router

	BucketService platform.BucketService
}

// NewBucketHandler returns a new instance of BucketHandler.
func NewBucketHandler() *BucketHandler {
	h := &BucketHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v1/buckets", h.handlePostBucket)
	h.HandlerFunc("GET", "/v1/buckets", h.handleGetBuckets)
	h.HandlerFunc("GET", "/v1/buckets/:id", h.handleGetBucket)
	h.HandlerFunc("PATCH", "/v1/buckets/:id", h.handlePatchBucket)
	h.HandlerFunc("DELETE", "/v1/buckets/:id", h.handleDeleteBucket)
	return h
}

type bucketResponse struct {
	Links map[string]string `json:"links"`
	platform.Bucket
}

func newBucketResponse(b *platform.Bucket) *bucketResponse {
	return &bucketResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/v1/buckets/%s", b.ID),
			"org":  fmt.Sprintf("/v1/orgs/%s", b.OrganizationID),
		},
		Bucket: *b,
	}
}

type bucketsResponse struct {
	Links   map[string]string `json:"links"`
	Buckets []*bucketResponse `json:"buckets"`
}

func newBucketsResponse(opts platform.FindOptions, f platform.BucketFilter, bs []*platform.Bucket) *bucketsResponse {
	rs := make([]*bucketResponse, 0, len(bs))
	for _, b := range bs {
		rs = append(rs, newBucketResponse(b))
	}
	return &bucketsResponse{
		// TODO(desa): update links to include paging and filter information
		Links: map[string]string{
			"self": "/v1/buckets",
		},
		Buckets: rs,
	}
}

// handlePostBucket is the HTTP handler for the POST /v1/buckets route.
func (h *BucketHandler) handlePostBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BucketService.CreateBucket(ctx, req.Bucket); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newBucketResponse(req.Bucket)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postBucketRequest struct {
	Bucket *platform.Bucket
}

func decodePostBucketRequest(ctx context.Context, r *http.Request) (*postBucketRequest, error) {
	b := &platform.Bucket{}

	queryParams := r.URL.Query()
	orgName := queryParams.Get("org")
	if orgName == "" {
		return nil, errors.New("The \"org\" is required via query param.")
	}

	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}
	b.Type = platform.BucketTypeUser
	b.Organization = orgName

	return &postBucketRequest{
		Bucket: b,
	}, nil
}

// handleGetBucket is the HTTP handler for the GET /v1/buckets/:id route.
func (h *BucketHandler) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.BucketService.FindBucketByID(ctx, req.BucketID)
	if err != nil {
		// TODO(desa): fix this when using real errors library
		if strings.Contains(err.Error(), "not found") {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getBucketRequest struct {
	BucketID platform.ID
}

func decodeGetBucketRequest(ctx context.Context, r *http.Request) (*getBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &getBucketRequest{
		BucketID: i,
	}

	return req, nil
}

// handleDeleteBucket is the HTTP handler for the DELETE /v1/buckets/:id route.
func (h *BucketHandler) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BucketService.DeleteBucket(ctx, req.BucketID); err != nil {
		// TODO(desa): fix this when using real errors library
		if strings.Contains(err.Error(), "not found") {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteBucketRequest struct {
	BucketID platform.ID
}

func decodeDeleteBucketRequest(ctx context.Context, r *http.Request) (*deleteBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteBucketRequest{
		BucketID: i,
	}

	return req, nil
}

// handleGetBuckets is the HTTP handler for the GET /v1/buckets route.
func (h *BucketHandler) handleGetBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetBucketsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	opts := platform.FindOptions{}
	bs, _, err := h.BucketService.FindBuckets(ctx, req.filter, opts)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketsResponse(opts, req.filter, bs)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getBucketsRequest struct {
	filter platform.BucketFilter
}

func decodeGetBucketsRequest(ctx context.Context, r *http.Request) (*getBucketsRequest, error) {
	qp := r.URL.Query()
	req := &getBucketsRequest{}

	if id := qp.Get("orgID"); id != "" {
		temp, err := platform.IDFromString(id)
		if err != nil {
			return nil, err
		}
		req.filter.OrganizationID = temp
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	if id := qp.Get("id"); id != "" {
		temp, err := platform.IDFromString(id)
		if err != nil {
			return nil, err
		}
		req.filter.ID = temp
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handlePatchBucket is the HTTP handler for the PATH /v1/buckets route.
func (h *BucketHandler) handlePatchBucket(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchBucketRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.BucketService.UpdateBucket(ctx, req.BucketID, req.Update)
	if err != nil {
		// TODO(desa): fix this when using real errors library
		if strings.Contains(err.Error(), "not found") {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketResponse(b)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchBucketRequest struct {
	Update   platform.BucketUpdate
	BucketID platform.ID
}

func decodePatchBucketRequest(ctx context.Context, r *http.Request) (*patchBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.BucketUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchBucketRequest{
		Update:   upd,
		BucketID: i,
	}, nil
}

const (
	bucketPath = "/v1/buckets"
)

// BucketService connects to Influx via HTTP using tokens to manage buckets
type BucketService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketService) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.Bucket
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// FindBucket returns the first bucket that matches filter.
func (s *BucketService) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	// don't expose internal buckets
	filter.Type = platform.BucketTypeUser

	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, ErrNotFound
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	// don't expose internal buckets
	filter.Type = platform.BucketTypeUser

	u, err := newURL(s.Addr, bucketPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()
	if filter.OrganizationID != nil {
		query.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Organization != nil {
		query.Add("org", *filter.Organization)
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}
	if filter.Name != nil {
		query.Add("name", *filter.Name)
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var bs bucketsResponse
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	buckets := make([]*platform.Bucket, 0, len(bs.Buckets))
	for _, b := range bs.Buckets {
		buckets = append(buckets, &b.Bucket)
	}

	return buckets, len(buckets), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketService) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	u, err := newURL(s.Addr, bucketPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(b)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(b); err != nil {
		return err
	}

	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketService) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.Bucket
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// DeleteBucket removes a bucket by ID.
func (s *BucketService) DeleteBucket(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, bucketIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func bucketIDPath(id platform.ID) string {
	return path.Join(bucketPath, id.String())
}
