package rtree

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dhconnelly/rtreego"
	gocache "github.com/patrickmn/go-cache"
	"github.com/skelterjohn/geom"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-spatial"
	"github.com/whosonfirst/go-whosonfirst-spatial/cache"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"github.com/whosonfirst/go-whosonfirst-spr"
	"net/url"
	"strconv"
	"sync"
	"time"
)

func init() {
	ctx := context.Background()
	database.RegisterSpatialDatabase(ctx, "rtree", NewRTreeSpatialDatabase)
}

// PLEASE DISCUSS WHY patrickm/go-cache AND NOT whosonfirst/go-cache HERE

type RTreeSpatialDatabase struct {
	database.SpatialDatabase
	Logger  *log.WOFLogger
	rtree   *rtreego.Rtree
	gocache *gocache.Cache
	mu      *sync.RWMutex
	strict  bool
}

type RTreeSpatialIndex struct {
	bounds *rtreego.Rect
	Id     string
}

func (sp RTreeSpatialIndex) Bounds() *rtreego.Rect {
	return sp.bounds
}

type RTreeResults struct {
	spr.StandardPlacesResults `json:",omitempty"`
	Places                    []spr.StandardPlacesResult `json:"places"`
}

func (r *RTreeResults) Results() []spr.StandardPlacesResult {
	return r.Places
}

func NewRTreeSpatialDatabase(ctx context.Context, uri string) (database.SpatialDatabase, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	q := u.Query()

	strict := true

	if q.Get("strict") == "false" {
		strict = false
	}

	expires := 0 * time.Second
	cleanup := 0 * time.Second

	str_exp := q.Get("default_expiration")
	str_cleanup := q.Get("cleanup_interval")

	if str_exp != "" {

		int_expires, err := strconv.Atoi(str_exp)

		if err != nil {
			return nil, err
		}

		expires = time.Duration(int_expires) * time.Second
	}

	if str_cleanup != "" {

		int_cleanup, err := strconv.Atoi(str_cleanup)

		if err != nil {
			return nil, err
		}

		cleanup = time.Duration(int_cleanup) * time.Second
	}

	gc := gocache.New(expires, cleanup)

	logger := log.SimpleWOFLogger("index")

	rtree := rtreego.NewTree(2, 25, 50)

	mu := new(sync.RWMutex)

	db := &RTreeSpatialDatabase{
		Logger:  logger,
		rtree:   rtree,
		gocache: gc,
		strict:  strict,
		mu:      mu,
	}

	return db, nil
}

func (r *RTreeSpatialDatabase) Close(ctx context.Context) error {

	return nil
}

func (r *RTreeSpatialDatabase) IndexFeature(ctx context.Context, f wof_geojson.Feature) error {

	str_id := f.Id()

	bboxes, err := f.BoundingBoxes()

	if err != nil {
		return err
	}

	err = r.setSPRCacheItem(ctx, f)

	if err != nil {
		return err
	}

	for _, bbox := range bboxes.Bounds() {

		sw := bbox.Min
		ne := bbox.Max

		llat := ne.Y - sw.Y
		llon := ne.X - sw.X

		pt := rtreego.Point{sw.X, sw.Y}
		rect, err := rtreego.NewRect(pt, []float64{llon, llat})

		if err != nil {

			if r.strict {
				return err
			}

			r.Logger.Error("%s failed indexing, (%v). Strict mode is disabled, so skipping.", str_id, err)
			return nil
		}

		r.Logger.Status("index %s %v", str_id, rect)

		sp := RTreeSpatialIndex{
			bounds: rect,
			Id:     str_id,
		}

		r.mu.Lock()
		r.rtree.Insert(&sp)
		r.mu.Unlock()
	}

	return nil
}

func (r *RTreeSpatialDatabase) PointInPolygon(ctx context.Context, coord *geom.Coord, filters ...filter.Filter) (spr.StandardPlacesResults, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan spr.StandardPlacesResult)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	results := make([]spr.StandardPlacesResult, 0)
	working := true

	go r.PointInPolygonWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			results = append(results, rsp)
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}

		if !working {
			break
		}
	}

	spr_results := &RTreeResults{
		Places: results,
	}

	return spr_results, nil
}

func (r *RTreeSpatialDatabase) PointInPolygonWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, done_ch chan bool, coord *geom.Coord, filters ...filter.Filter) {

	defer func() {
		done_ch <- true
	}()

	rows, err := r.getIntersectsByCoord(coord)

	if err != nil {
		err_ch <- err
		return
	}

	r.inflateResultsWithChannels(ctx, rsp_ch, err_ch, rows, coord, filters...)
	return
}

func (r *RTreeSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *geom.Coord) ([]*spatial.PointInPolygonCandidate, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan *spatial.PointInPolygonCandidate)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	candidates := make([]*spatial.PointInPolygonCandidate, 0)
	working := true

	go r.PointInPolygonCandidatesWithChannels(ctx, coord, rsp_ch, err_ch, done_ch)

	for {
		select {
		case <-ctx.Done():
			return nil, nil
		case <-done_ch:
			working = false
		case rsp := <-rsp_ch:
			candidates = append(candidates, rsp)
		case err := <-err_ch:
			return nil, err
		default:
			// pass
		}

		if !working {
			break
		}
	}

	return candidates, nil
}

func (r *RTreeSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, coord *geom.Coord, rsp_ch chan *spatial.PointInPolygonCandidate, err_ch chan error, done_ch chan bool) {

	defer func() {
		done_ch <- true
	}()

	intersects, err := r.getIntersectsByCoord(coord)

	if err != nil {
		err_ch <- err
		return
	}

	for _, raw := range intersects {

		sp := raw.(*RTreeSpatialIndex)
		str_id := sp.Id

		bounds := sp.Bounds()

		c := &spatial.PointInPolygonCandidate{
			Id:       sp.Id,
			WOFId:    sp.WOFId,
			AltLabel: sp.AltLabel,
			Bounds:   &bounds,
		}

		rsp_ch <- candidate
	}

	return
}

func (r *RTreeSpatialDatabase) getIntersectsByCoord(coord *geom.Coord) ([]rtreego.Spatial, error) {

	lat := coord.Y
	lon := coord.X

	pt := rtreego.Point{lon, lat}
	rect, err := rtreego.NewRect(pt, []float64{0.0001, 0.0001}) // how small can I make this?

	if err != nil {
		return nil, err
	}

	return r.getIntersectsByRect(rect)
}

func (r *RTreeSpatialDatabase) getIntersectsByRect(rect *rtreego.Rect) ([]rtreego.Spatial, error) {

	// to do: timings that don't slow everything down the way
	// go-whosonfirst-timer does now (20170915/thisisaaronland)

	results := r.rtree.SearchIntersect(rect)
	return results, nil
}

func (r *RTreeSpatialDatabase) inflateResultsWithChannels(ctx context.Context, rsp_ch chan spr.StandardPlacesResult, err_ch chan error, possible []rtreego.Spatial, c *geom.Coord, filters ...filter.Filter) {

	seen := make(map[string]bool)

	mu := new(sync.RWMutex)
	wg := new(sync.WaitGroup)

	for _, row := range possible {

		sp := row.(*RTreeSpatialIndex)
		wg.Add(1)

		go func(sp *RTreeSpatialIndex) {

			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			str_id := sp.Id

			mu.RLock()
			_, ok := seen[str_id]
			mu.RUnlock()

			if ok {
				return
			}

			mu.Lock()
			seen[str_id] = true
			mu.Unlock()

			fc, err := r.retrieveSPRCacheItem(ctx, str_id)

			if err != nil {
				r.Logger.Error("Failed to retrieve feature cache for %s, %v", str_id, err)
				return
			}

			s, err := fc.SPR()

			if err != nil {
				r.Logger.Error("Failed to retrieve feature SPR for %s, %v", str_id, err)
				return
			}

			for _, f := range filters {

				err = filter.FilterSPR(f, s)

				if err != nil {
					r.Logger.Debug("SKIP %s because filter error %s", str_id, err)
					return
				}
			}

			geom, err := fc.Geometry()

			if err != nil {
				r.Logger.Error("Failed to retrieve feature geometry for %s, %v", str_id, err)
				return
			}

			contains := geo.GeoJSONGeometryContainsCoord(geom, c)

			if !contains {
				r.Logger.Debug("SKIP %s because does not contain coord (%v)", str_id, c)
				return
			}

			rsp_ch <- s
		}(sp)
	}

	wg.Wait()
}

func (r *RTreeSpatialDatabase) setSPRCacheItem(ctx context.Context, f wof_geojson.Feature) error {

	fc, err := cache.NewSPRCacheItem(f)

	if err != nil {
		return err
	}

	r.gocache.Set(f.Id(), fc, -1)
	return nil
}

func (r *RTreeSpatialDatabase) retrieveSPRCacheItem(ctx context.Context, str_id string) (*cache.SPRCacheItem, error) {

	fc, ok := r.gocache.Get(str_id)

	if !ok {
		return nil, errors.New("Invalid cache ID")
	}

	return fc.(*cache.SPRCacheItem), nil
}
