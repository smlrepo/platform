package pb

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/functions/inputs/storage"
	"github.com/influxdata/flux/values"
	ostorage "github.com/influxdata/influxdb/services/storage"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func NewReader(hl storage.HostLookup) (*reader, error) {
	tracer := opentracing.GlobalTracer()

	// TODO(nathanielc): Watch for host changes
	hosts := hl.Hosts()
	conns := make([]connection, len(hosts))
	for i, h := range hosts {
		conn, err := grpc.Dial(
			h,
			grpc.WithInsecure(),
			grpc.WithUnaryInterceptor(otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(otgrpc.OpenTracingStreamClientInterceptor(tracer)),
		)
		if err != nil {
			return nil, err
		}
		conns[i] = connection{
			host:   h,
			conn:   conn,
			client: ostorage.NewStorageClient(conn),
		}
	}
	return &reader{
		conns: conns,
	}, nil
}

type reader struct {
	conns []connection
}

type connection struct {
	host   string
	conn   *grpc.ClientConn
	client ostorage.StorageClient
}

func (sr *reader) Read(ctx context.Context, readSpec storage.ReadSpec, start, stop execute.Time) (flux.TableIterator, error) {
	var predicate *ostorage.Predicate
	if readSpec.Predicate != nil {
		p, err := ToStoragePredicate(readSpec.Predicate)
		if err != nil {
			return nil, err
		}
		predicate = p
	}

	bi := &tableIterator{
		ctx: ctx,
		bounds: execute.Bounds{
			Start: start,
			Stop:  stop,
		},
		conns:     sr.conns,
		readSpec:  readSpec,
		predicate: predicate,
	}
	return bi, nil
}

func (sr *reader) Close() {
	for _, conn := range sr.conns {
		_ = conn.conn.Close()
	}
}

type tableIterator struct {
	ctx       context.Context
	bounds    execute.Bounds
	conns     []connection
	readSpec  storage.ReadSpec
	predicate *ostorage.Predicate
}

func (bi *tableIterator) Do(f func(flux.Table) error) error {
	src := ostorage.ReadSource{Database: string(bi.readSpec.BucketID)}
	if i := strings.IndexByte(src.Database, '/'); i > -1 {
		src.RetentionPolicy = src.Database[i+1:]
		src.Database = src.Database[:i]
	}

	// Setup read request
	var req ostorage.ReadRequest
	if any, err := types.MarshalAny(&src); err != nil {
		return err
	} else {
		req.ReadSource = any
	}
	req.Predicate = bi.predicate
	req.Descending = bi.readSpec.Descending
	req.TimestampRange.Start = int64(bi.bounds.Start)
	req.TimestampRange.End = int64(bi.bounds.Stop)
	req.Group = convertGroupMode(bi.readSpec.GroupMode)
	req.GroupKeys = bi.readSpec.GroupKeys
	req.SeriesLimit = bi.readSpec.SeriesLimit
	req.PointsLimit = bi.readSpec.PointsLimit
	req.SeriesOffset = bi.readSpec.SeriesOffset

	if req.PointsLimit == -1 {
		req.Hints.SetNoPoints()
	}

	if agg, err := determineAggregateMethod(bi.readSpec.AggregateMethod); err != nil {
		return err
	} else if agg != ostorage.AggregateTypeNone {
		req.Aggregate = &ostorage.Aggregate{Type: agg}
	}
	isGrouping := req.Group != ostorage.GroupAll
	streams := make([]*streamState, 0, len(bi.conns))
	for _, c := range bi.conns {
		if len(bi.readSpec.Hosts) > 0 {
			// Filter down to only hosts provided
			found := false
			for _, h := range bi.readSpec.Hosts {
				if c.host == h {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		stream, err := c.client.Read(bi.ctx, &req)
		if err != nil {
			return err
		}
		streams = append(streams, &streamState{
			bounds:   bi.bounds,
			stream:   stream,
			readSpec: &bi.readSpec,
			group:    isGrouping,
		})
	}

	ms := &mergedStreams{
		streams: streams,
	}

	if isGrouping {
		return bi.handleGroupRead(f, ms)
	}
	return bi.handleRead(f, ms)
}

func (bi *tableIterator) handleRead(f func(flux.Table) error, ms *mergedStreams) error {
	for ms.more() {
		if p := ms.peek(); readFrameType(p) != seriesType {
			//This means the consumer didn't read all the data off the table
			return errors.New("internal error: short read")
		}
		frame := ms.next()
		s := frame.GetSeries()
		typ := convertDataType(s.DataType)
		key := groupKeyForSeries(s, &bi.readSpec, bi.bounds)
		cols, defs := determineTableColsForSeries(s, typ)
		table := newTable(bi.bounds, key, cols, ms, &bi.readSpec, s.Tags, defs)

		if err := f(table); err != nil {
			// TODO(nathanielc): Close streams since we have abandoned the request
			return err
		}
		// Wait until the table has been read.
		table.wait()
	}
	return nil
}

func (bi *tableIterator) handleGroupRead(f func(flux.Table) error, ms *mergedStreams) error {
	for ms.more() {
		if p := ms.peek(); readFrameType(p) != groupType {
			//This means the consumer didn't read all the data off the table
			return errors.New("internal error: short read")
		}
		frame := ms.next()
		s := frame.GetGroup()
		key := groupKeyForGroup(s, &bi.readSpec, bi.bounds)

		// try to infer type
		// TODO(sgc): this is a hack
		typ := flux.TString
		if p := ms.peek(); readFrameType(p) == seriesType {
			typ = convertDataType(p.GetSeries().DataType)
		}
		cols, defs := determineTableColsForGroup(s, typ)

		table := newTable(bi.bounds, key, cols, ms, &bi.readSpec, nil, defs)

		if err := f(table); err != nil {
			// TODO(nathanielc): Close streams since we have abandoned the request
			return err
		}
		// Wait until the table has been read.
		table.wait()
	}
	return nil
}

func determineAggregateMethod(agg string) (ostorage.Aggregate_AggregateType, error) {
	if agg == "" {
		return ostorage.AggregateTypeNone, nil
	}

	if t, ok := ostorage.Aggregate_AggregateType_value[strings.ToUpper(agg)]; ok {
		return ostorage.Aggregate_AggregateType(t), nil
	}
	return 0, fmt.Errorf("unknown aggregate type %q", agg)
}

func convertGroupMode(m storage.GroupMode) ostorage.ReadRequest_Group {
	switch m {
	case storage.GroupModeNone:
		return ostorage.GroupNone
	case storage.GroupModeBy:
		return ostorage.GroupBy
	case storage.GroupModeExcept:
		return ostorage.GroupExcept

	case storage.GroupModeDefault, storage.GroupModeAll:
		fallthrough
	default:
		return ostorage.GroupAll
	}
}

func convertDataType(t ostorage.ReadResponse_DataType) flux.DataType {
	switch t {
	case ostorage.DataTypeFloat:
		return flux.TFloat
	case ostorage.DataTypeInteger:
		return flux.TInt
	case ostorage.DataTypeUnsigned:
		return flux.TUInt
	case ostorage.DataTypeBoolean:
		return flux.TBool
	case ostorage.DataTypeString:
		return flux.TString
	default:
		return flux.TInvalid
	}
}

const (
	startColIdx = 0
	stopColIdx  = 1
	timeColIdx  = 2
	valueColIdx = 3
)

func determineTableColsForSeries(s *ostorage.ReadResponse_SeriesFrame, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(s.Tags))
	defs := make([][]byte, 4+len(s.Tags))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range s.Tags {
		cols[4+j] = flux.ColMeta{
			Label: string(tag.Key),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")
	}
	return cols, defs
}

func groupKeyForSeries(s *ostorage.ReadResponse_SeriesFrame, readSpec *storage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(s.Tags))
	vs := make([]values.Value, 2, len(s.Tags))
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	switch readSpec.GroupMode {
	case storage.GroupModeBy:
		// group key in GroupKeys order, including tags in the GroupKeys slice
		for _, k := range readSpec.GroupKeys {
			if i := indexOfTag(s.Tags, k); i < len(s.Tags) {
				cols = append(cols, flux.ColMeta{
					Label: string(s.Tags[i].Key),
					Type:  flux.TString,
				})
				vs = append(vs, values.NewStringValue(string(s.Tags[i].Value)))
			}
		}
	case storage.GroupModeExcept:
		// group key in GroupKeys order, skipping tags in the GroupKeys slice
		for _, k := range readSpec.GroupKeys {
			if i := indexOfTag(s.Tags, k); i == len(s.Tags) {
				cols = append(cols, flux.ColMeta{
					Label: string(s.Tags[i].Key),
					Type:  flux.TString,
				})
				vs = append(vs, values.NewStringValue(string(s.Tags[i].Value)))
			}
		}
	case storage.GroupModeDefault, storage.GroupModeAll:
		for i := range s.Tags {
			cols = append(cols, flux.ColMeta{
				Label: string(s.Tags[i].Key),
				Type:  flux.TString,
			})
			vs = append(vs, values.NewStringValue(string(s.Tags[i].Value)))
		}
	}
	return execute.NewGroupKey(cols, vs)
}

func determineTableColsForGroup(f *ostorage.ReadResponse_GroupFrame, typ flux.DataType) ([]flux.ColMeta, [][]byte) {
	cols := make([]flux.ColMeta, 4+len(f.TagKeys))
	defs := make([][]byte, 4+len(f.TagKeys))
	cols[startColIdx] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	cols[stopColIdx] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	cols[timeColIdx] = flux.ColMeta{
		Label: execute.DefaultTimeColLabel,
		Type:  flux.TTime,
	}
	cols[valueColIdx] = flux.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  typ,
	}
	for j, tag := range f.TagKeys {
		cols[4+j] = flux.ColMeta{
			Label: string(tag),
			Type:  flux.TString,
		}
		defs[4+j] = []byte("")

	}
	return cols, defs
}

func groupKeyForGroup(g *ostorage.ReadResponse_GroupFrame, readSpec *storage.ReadSpec, bnds execute.Bounds) flux.GroupKey {
	cols := make([]flux.ColMeta, 2, len(readSpec.GroupKeys)+2)
	vs := make([]values.Value, 2, len(readSpec.GroupKeys)+2)
	cols[0] = flux.ColMeta{
		Label: execute.DefaultStartColLabel,
		Type:  flux.TTime,
	}
	vs[0] = values.NewTimeValue(bnds.Start)
	cols[1] = flux.ColMeta{
		Label: execute.DefaultStopColLabel,
		Type:  flux.TTime,
	}
	vs[1] = values.NewTimeValue(bnds.Stop)
	for i := range readSpec.GroupKeys {
		cols = append(cols, flux.ColMeta{
			Label: readSpec.GroupKeys[i],
			Type:  flux.TString,
		})
		vs = append(vs, values.NewStringValue(string(g.PartitionKeyVals[i])))
	}
	return execute.NewGroupKey(cols, vs)
}

// table implement OneTimeTable as it can only be read once.
// Since it can only be read once it is also a ValueIterator for itself.
type table struct {
	bounds execute.Bounds
	key    flux.GroupKey
	cols   []flux.ColMeta

	empty bool
	more  bool

	// cache of the tags on the current series.
	// len(tags) == len(colMeta)
	tags [][]byte
	defs [][]byte

	readSpec *storage.ReadSpec

	done chan struct{}

	ms *mergedStreams

	// The current number of records in memory
	l int
	// colBufs are the buffers for the given columns.
	colBufs []interface{}

	// resuable buffer for the time column
	timeBuf []execute.Time

	// resuable buffers for the different types of values
	boolBuf   []bool
	intBuf    []int64
	uintBuf   []uint64
	floatBuf  []float64
	stringBuf []string

	err error
}

func newTable(
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	ms *mergedStreams,
	readSpec *storage.ReadSpec,
	tags []ostorage.Tag,
	defs [][]byte,
) *table {
	b := &table{
		bounds:   bounds,
		key:      key,
		tags:     make([][]byte, len(cols)),
		defs:     defs,
		colBufs:  make([]interface{}, len(cols)),
		cols:     cols,
		readSpec: readSpec,
		ms:       ms,
		done:     make(chan struct{}),
		empty:    true,
	}
	b.readTags(tags)
	// Call advance now so that we know if we are empty or not
	b.more = b.advance()
	return b
}

func (t *table) RefCount(n int) {
	//TODO(nathanielc): Have the table consume the Allocator,
	// once we have zero-copy serialization over the network
}

func (t *table) Err() error { return t.err }

func (t *table) wait() {
	<-t.done
}

func (t *table) Key() flux.GroupKey {
	return t.key
}
func (t *table) Cols() []flux.ColMeta {
	return t.cols
}

// onetime satisfies the OneTimeTable interface since this table may only be read once.
func (t *table) onetime() {}
func (t *table) Do(f func(flux.ColReader) error) error {
	defer close(t.done)
	// If the initial advance call indicated we are done, return immediately
	if !t.more {
		return t.err
	}

	f(t)
	for t.advance() {
		if err := f(t); err != nil {
			return err
		}
	}
	return t.err
}

func (t *table) Len() int {
	return t.l
}

func (t *table) Bools(j int) []bool {
	execute.CheckColType(t.cols[j], flux.TBool)
	return t.colBufs[j].([]bool)
}
func (t *table) Ints(j int) []int64 {
	execute.CheckColType(t.cols[j], flux.TInt)
	return t.colBufs[j].([]int64)
}
func (t *table) UInts(j int) []uint64 {
	execute.CheckColType(t.cols[j], flux.TUInt)
	return t.colBufs[j].([]uint64)
}
func (t *table) Floats(j int) []float64 {
	execute.CheckColType(t.cols[j], flux.TFloat)
	return t.colBufs[j].([]float64)
}
func (t *table) Strings(j int) []string {
	execute.CheckColType(t.cols[j], flux.TString)
	return t.colBufs[j].([]string)
}
func (t *table) Times(j int) []execute.Time {
	execute.CheckColType(t.cols[j], flux.TTime)
	return t.colBufs[j].([]execute.Time)
}

// readTags populates b.tags with the provided tags
func (t *table) readTags(tags []ostorage.Tag) {
	for j := range t.tags {
		t.tags[j] = t.defs[j]
	}

	if len(tags) == 0 {
		return
	}

	for _, tag := range tags {
		k := string(tag.Key)
		j := execute.ColIdx(k, t.cols)
		t.tags[j] = tag.Value
	}
}

func (t *table) advance() bool {
	for t.ms.more() {
		//reset buffers
		t.timeBuf = t.timeBuf[0:0]
		t.boolBuf = t.boolBuf[0:0]
		t.intBuf = t.intBuf[0:0]
		t.uintBuf = t.uintBuf[0:0]
		t.stringBuf = t.stringBuf[0:0]
		t.floatBuf = t.floatBuf[0:0]

		switch p := t.ms.peek(); readFrameType(p) {
		case groupType:
			return false
		case seriesType:
			if !t.ms.key().Equal(t.key) {
				// We have reached the end of data for this table
				return false
			}
			s := p.GetSeries()
			t.readTags(s.Tags)

			// Advance to next frame
			t.ms.next()

			if t.readSpec.PointsLimit == -1 {
				// do not expect points frames
				t.l = 0
				return true
			}
		case boolPointsType:
			if t.cols[valueColIdx].Type != flux.TBool {
				t.err = fmt.Errorf("value type changed from %s -> %s", t.cols[valueColIdx].Type, flux.TBool)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			t.empty = false
			// read next frame
			frame := t.ms.next()
			p := frame.GetBooleanPoints()
			l := len(p.Timestamps)
			t.l = l
			if l > cap(t.timeBuf) {
				t.timeBuf = make([]execute.Time, l)
			} else {
				t.timeBuf = t.timeBuf[:l]
			}
			if l > cap(t.boolBuf) {
				t.boolBuf = make([]bool, l)
			} else {
				t.boolBuf = t.boolBuf[:l]
			}

			for i, c := range p.Timestamps {
				t.timeBuf[i] = execute.Time(c)
				t.boolBuf[i] = p.Values[i]
			}
			t.colBufs[timeColIdx] = t.timeBuf
			t.colBufs[valueColIdx] = t.boolBuf
			t.appendTags()
			t.appendBounds()
			return true
		case intPointsType:
			if t.cols[valueColIdx].Type != flux.TInt {
				t.err = fmt.Errorf("value type changed from %s -> %s", t.cols[valueColIdx].Type, flux.TInt)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			t.empty = false
			// read next frame
			frame := t.ms.next()
			p := frame.GetIntegerPoints()
			l := len(p.Timestamps)
			t.l = l
			if l > cap(t.timeBuf) {
				t.timeBuf = make([]execute.Time, l)
			} else {
				t.timeBuf = t.timeBuf[:l]
			}
			if l > cap(t.uintBuf) {
				t.intBuf = make([]int64, l)
			} else {
				t.intBuf = t.intBuf[:l]
			}

			for i, c := range p.Timestamps {
				t.timeBuf[i] = execute.Time(c)
				t.intBuf[i] = p.Values[i]
			}
			t.colBufs[timeColIdx] = t.timeBuf
			t.colBufs[valueColIdx] = t.intBuf
			t.appendTags()
			t.appendBounds()
			return true
		case uintPointsType:
			if t.cols[valueColIdx].Type != flux.TUInt {
				t.err = fmt.Errorf("value type changed from %s -> %s", t.cols[valueColIdx].Type, flux.TUInt)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			t.empty = false
			// read next frame
			frame := t.ms.next()
			p := frame.GetUnsignedPoints()
			l := len(p.Timestamps)
			t.l = l
			if l > cap(t.timeBuf) {
				t.timeBuf = make([]execute.Time, l)
			} else {
				t.timeBuf = t.timeBuf[:l]
			}
			if l > cap(t.intBuf) {
				t.uintBuf = make([]uint64, l)
			} else {
				t.uintBuf = t.uintBuf[:l]
			}

			for i, c := range p.Timestamps {
				t.timeBuf[i] = execute.Time(c)
				t.uintBuf[i] = p.Values[i]
			}
			t.colBufs[timeColIdx] = t.timeBuf
			t.colBufs[valueColIdx] = t.uintBuf
			t.appendTags()
			t.appendBounds()
			return true
		case floatPointsType:
			if t.cols[valueColIdx].Type != flux.TFloat {
				t.err = fmt.Errorf("value type changed from %s -> %s", t.cols[valueColIdx].Type, flux.TFloat)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			t.empty = false
			// read next frame
			frame := t.ms.next()
			p := frame.GetFloatPoints()

			l := len(p.Timestamps)
			t.l = l
			if l > cap(t.timeBuf) {
				t.timeBuf = make([]execute.Time, l)
			} else {
				t.timeBuf = t.timeBuf[:l]
			}
			if l > cap(t.floatBuf) {
				t.floatBuf = make([]float64, l)
			} else {
				t.floatBuf = t.floatBuf[:l]
			}

			for i, c := range p.Timestamps {
				t.timeBuf[i] = execute.Time(c)
				t.floatBuf[i] = p.Values[i]
			}
			t.colBufs[timeColIdx] = t.timeBuf
			t.colBufs[valueColIdx] = t.floatBuf
			t.appendTags()
			t.appendBounds()
			return true
		case stringPointsType:
			if t.cols[valueColIdx].Type != flux.TString {
				t.err = fmt.Errorf("value type changed from %s -> %s", t.cols[valueColIdx].Type, flux.TString)
				// TODO: Add error handling
				// Type changed,
				return false
			}
			t.empty = false
			// read next frame
			frame := t.ms.next()
			p := frame.GetStringPoints()

			l := len(p.Timestamps)
			t.l = l
			if l > cap(t.timeBuf) {
				t.timeBuf = make([]execute.Time, l)
			} else {
				t.timeBuf = t.timeBuf[:l]
			}
			if l > cap(t.stringBuf) {
				t.stringBuf = make([]string, l)
			} else {
				t.stringBuf = t.stringBuf[:l]
			}

			for i, c := range p.Timestamps {
				t.timeBuf[i] = execute.Time(c)
				t.stringBuf[i] = p.Values[i]
			}
			t.colBufs[timeColIdx] = t.timeBuf
			t.colBufs[valueColIdx] = t.stringBuf
			t.appendTags()
			t.appendBounds()
			return true
		}
	}
	return false
}

// appendTags fills the colBufs for the tag columns with the tag value.
func (t *table) appendTags() {
	for j := range t.cols {
		v := t.tags[j]
		if v != nil {
			if t.colBufs[j] == nil {
				t.colBufs[j] = make([]string, t.l)
			}
			colBuf := t.colBufs[j].([]string)
			if cap(colBuf) < t.l {
				colBuf = make([]string, t.l)
			} else {
				colBuf = colBuf[:t.l]
			}
			vStr := string(v)
			for i := range colBuf {
				colBuf[i] = vStr
			}
			t.colBufs[j] = colBuf
		}
	}
}

// appendBounds fills the colBufs for the time bounds
func (t *table) appendBounds() {
	bounds := []execute.Time{t.bounds.Start, t.bounds.Stop}
	for j := range []int{startColIdx, stopColIdx} {
		if t.colBufs[j] == nil {
			t.colBufs[j] = make([]execute.Time, t.l)
		}
		colBuf := t.colBufs[j].([]execute.Time)
		if cap(colBuf) < t.l {
			colBuf = make([]execute.Time, t.l)
		} else {
			colBuf = colBuf[:t.l]
		}
		for i := range colBuf {
			colBuf[i] = bounds[j]
		}
		t.colBufs[j] = colBuf
	}
}

func (t *table) Empty() bool {
	return t.empty
}

type streamState struct {
	bounds     execute.Bounds
	stream     ostorage.Storage_ReadClient
	rep        ostorage.ReadResponse
	currentKey flux.GroupKey
	readSpec   *storage.ReadSpec
	finished   bool
	group      bool
}

func (s *streamState) peek() ostorage.ReadResponse_Frame {
	return s.rep.Frames[0]
}

func (s *streamState) more() bool {
	if s.finished {
		return false
	}
	if len(s.rep.Frames) > 0 {
		return true
	}
	if err := s.stream.RecvMsg(&s.rep); err != nil {
		s.finished = true
		if err == io.EOF {
			// We are done
			return false
		}
		//TODO add proper error handling
		return false
	}
	if len(s.rep.Frames) == 0 {
		return false
	}
	s.computeKey()
	return true
}

func (s *streamState) key() flux.GroupKey {
	return s.currentKey
}

func (s *streamState) computeKey() {
	// Determine new currentKey
	p := s.peek()
	ft := readFrameType(p)
	if s.group {
		if ft == groupType {
			group := p.GetGroup()
			s.currentKey = groupKeyForGroup(group, s.readSpec, s.bounds)
		}
	} else {
		if ft == seriesType {
			series := p.GetSeries()
			s.currentKey = groupKeyForSeries(series, s.readSpec, s.bounds)
		}
	}
}

func (s *streamState) next() ostorage.ReadResponse_Frame {
	frame := s.rep.Frames[0]
	s.rep.Frames = s.rep.Frames[1:]
	if len(s.rep.Frames) > 0 {
		s.computeKey()
	}
	return frame
}

type mergedStreams struct {
	streams    []*streamState
	currentKey flux.GroupKey
	i          int
}

func (s *mergedStreams) key() flux.GroupKey {
	if len(s.streams) == 1 {
		return s.streams[0].key()
	}
	return s.currentKey
}
func (s *mergedStreams) peek() ostorage.ReadResponse_Frame {
	return s.streams[s.i].peek()
}

func (s *mergedStreams) next() ostorage.ReadResponse_Frame {
	return s.streams[s.i].next()
}

func (s *mergedStreams) more() bool {
	// Optimze for the case of just one stream
	if len(s.streams) == 1 {
		return s.streams[0].more()
	}
	if s.i < 0 {
		return false
	}
	if s.currentKey == nil {
		return s.determineNewKey()
	}
	if s.streams[s.i].more() {
		if s.streams[s.i].key().Equal(s.currentKey) {
			return true
		}
		return s.advance()
	}
	return s.advance()
}

func (s *mergedStreams) advance() bool {
	s.i++
	if s.i == len(s.streams) {
		if !s.determineNewKey() {
			// no new data on any stream
			return false
		}
	}
	return s.more()
}

func (s *mergedStreams) determineNewKey() bool {
	minIdx := -1
	var minKey flux.GroupKey
	for i, stream := range s.streams {
		if !stream.more() {
			continue
		}
		k := stream.key()
		if minIdx == -1 || k.Less(minKey) {
			minIdx = i
			minKey = k
		}
	}
	s.currentKey = minKey
	s.i = minIdx
	return s.i >= 0
}

type frameType int

const (
	seriesType frameType = iota
	groupType
	boolPointsType
	intPointsType
	uintPointsType
	floatPointsType
	stringPointsType
)

func readFrameType(frame ostorage.ReadResponse_Frame) frameType {
	switch frame.Data.(type) {
	case *ostorage.ReadResponse_Frame_Series:
		return seriesType
	case *ostorage.ReadResponse_Frame_Group:
		return groupType
	case *ostorage.ReadResponse_Frame_BooleanPoints:
		return boolPointsType
	case *ostorage.ReadResponse_Frame_IntegerPoints:
		return intPointsType
	case *ostorage.ReadResponse_Frame_UnsignedPoints:
		return uintPointsType
	case *ostorage.ReadResponse_Frame_FloatPoints:
		return floatPointsType
	case *ostorage.ReadResponse_Frame_StringPoints:
		return stringPointsType
	default:
		panic(fmt.Errorf("unknown read response frame type: %T", frame.Data))
	}
}

func indexOfTag(t []ostorage.Tag, k string) int {
	return sort.Search(len(t), func(i int) bool { return string(t[i].Key) >= k })
}
