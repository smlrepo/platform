package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

const bucketPath = "/v1/buckets"

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
	h.HandlerFunc("GET", "/v1/buckets/:id/owners", h.handleGetOwners)
	h.HandlerFunc("POST", "/v1/buckets/:id/owners", h.handlePostOwner)
	h.HandlerFunc("DELETE", "/v1/buckets/:id/owners/:oid", h.handleDeleteOwner)

	return h
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

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Bucket); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postBucketRequest struct {
	Bucket *platform.Bucket
}

func decodePostBucketRequest(ctx context.Context, r *http.Request) (*postBucketRequest, error) {
	b := &platform.Bucket{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

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
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
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
		return nil, kerrors.InvalidDataf("url missing id")
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
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteBucketRequest struct {
	BucketID platform.ID
}

func decodeDeleteBucketRequest(ctx context.Context, r *http.Request) (*deleteBucketRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
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

	bs, _, err := h.BucketService.FindBuckets(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, bs); err != nil {
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
		req.filter.OrganizationID = &platform.ID{}
		if err := req.filter.OrganizationID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	if id := qp.Get("id"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
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
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
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
		return nil, kerrors.InvalidDataf("url missing id")
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

func (h *BucketHandler) handleGetOwners(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetOwnersRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	owners, err := h.BucketService.GetBucketOwners(ctx, req.BucketID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, owners); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getOwnerRequest struct {
	BucketID platform.ID
}

func decodeGetOwnersRequest(ctx context.Context, r *http.Request) (*getOwnerRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &getOwnerRequest{
		BucketID: i,
	}

	return req, nil
}

func (h *BucketHandler) handlePostOwner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostOwnerRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BucketService.AddBucketOwner(ctx, req.BucketID, req.Owner); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Owner); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postOwnerRequest struct {
	BucketID platform.ID
	Owner    *platform.Owner
}

func decodePostOwnerRequest(ctx context.Context, r *http.Request) (*postOwnerRequest, error) {
	o := &platform.Owner{}
	if err := json.NewDecoder(r.Body).Decode(o); err != nil {
		return nil, err
	}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing bucket id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &postOwnerRequest{
		BucketID: i,
		Owner:    o,
	}, nil
}

func (h *BucketHandler) handleDeleteOwner(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteOwnerRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.BucketService.RemoveBucketOwner(ctx, req.BucketID, req.OwnerID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteOwnerRequest struct {
	BucketID platform.ID
	OwnerID  platform.ID
}

func decodeDeleteOwnerRequest(ctx context.Context, r *http.Request) (*deleteOwnerRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing bucket id")
	}

	var bucketID platform.ID
	if err := bucketID.DecodeFromString(id); err != nil {
		return nil, err
	}

	id = params.ByName("oid")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing owner id")
	}

	var ownerID platform.ID
	if err := ownerID.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &deleteOwnerRequest{
		BucketID: bucketID,
		OwnerID:  ownerID,
	}

	return req, nil
}

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
	req.Header.Set("Authorization", s.Token)

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
	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, errors.New("found no matching buckets")
	}

	return bs[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketService) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var bs []*platform.Bucket
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	return bs, len(bs), nil
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
	req.Header.Set("Authorization", s.Token)

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
	req.Header.Set("Authorization", s.Token)

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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func (s *BucketService) AddBucketOwner(ctx context.Context, bucketID platform.ID, owner platform.Owner) error {
	u, err := newURL(s.Addr, bucketOwnerPath(bucketID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(owner)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(owner); err != nil {
		return err
	}

	return nil
}

func (s *BucketService) GetBucketOwners(ctx context.Context, bucketID platform.ID) (*[]platform.Owner, error) {
	u, err := newURL(s.Addr, bucketOwnerPath(bucketID))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var owners *[]platform.Owner
	if err := json.NewDecoder(resp.Body).Decode(&owners); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return owners, nil
}

func (s *BucketService) RemoveBucketOwner(ctx context.Context, bucketID platform.ID, ownerID platform.ID) error {
	u, err := newURL(s.Addr, bucketOwnerIDPath(bucketID, ownerID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", s.Token)

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

func bucketOwnerPath(bucketID platform.ID) string {
	return path.Join(bucketIDPath(bucketID), "owners")
}

func bucketOwnerIDPath(bucketID platform.ID, ownerID platform.ID) string {
	return path.Join(bucketOwnerPath(bucketID), ownerID.String())
}
