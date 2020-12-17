package rtree

import (
	"context"
	_ "encoding/json"
	"errors"
	"fmt"
	"github.com/dhconnelly/rtreego"
	gocache "github.com/patrickmn/go-cache"
	pm_geojson "github.com/paulmach/go.geojson"
	"github.com/skelterjohn/geom"
	wof_geojson "github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/geometry"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-spatial"
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

type RTreeCache struct {
	Geometry *pm_geojson.Geometry
	SPR      spr.StandardPlacesResult
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

// cannot use &sp (type *RTreeSpatialIndex) as type rtreego.Spatial in argument to r.rtree.Insert:

type RTreeSpatialIndex struct {
	Rect     *rtreego.Rect
	Id       string
	WOFId    int64
	IsAlt    bool
	AltLabel string
}

func (i *RTreeSpatialIndex) Bounds() *rtreego.Rect {
	return i.Rect
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

	err := r.setCache(ctx, f)

	if err != nil {
		return err
	}

	is_alt := whosonfirst.IsAlt(f)
	alt_label := whosonfirst.AltLabel(f)

	wof_id := whosonfirst.Id(f)

	bboxes, err := f.BoundingBoxes()

	if err != nil {
		return err
	}

	for i, bbox := range bboxes.Bounds() {

		sp_id := fmt.Sprintf("%d:%s#%d", wof_id, alt_label, i)

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

			r.Logger.Error("%s failed indexing, (%v). Strict mode is disabled, so skipping.", sp_id, err)
			return nil
		}

		r.Logger.Status("index %s %v", sp_id, rect)

		sp := RTreeSpatialIndex{
			Rect:     rect,
			Id:       sp_id,
			WOFId:    wof_id,
			IsAlt:    is_alt,
			AltLabel: alt_label,
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

func (r *RTreeSpatialDatabase) PointInPolygonCandidates(ctx context.Context, coord *geom.Coord, filters ...filter.Filter) ([]*spatial.PointInPolygonCandidate, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsp_ch := make(chan *spatial.PointInPolygonCandidate)
	err_ch := make(chan error)
	done_ch := make(chan bool)

	candidates := make([]*spatial.PointInPolygonCandidate, 0)
	working := true

	go r.PointInPolygonCandidatesWithChannels(ctx, rsp_ch, err_ch, done_ch, coord, filters...)

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

func (r *RTreeSpatialDatabase) PointInPolygonCandidatesWithChannels(ctx context.Context, rsp_ch chan *spatial.PointInPolygonCandidate, err_ch chan error, done_ch chan bool, coord *geom.Coord, filters ...filter.Filter) {

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

		// bounds := sp.Bounds()

		c := &spatial.PointInPolygonCandidate{
			Id:       sp.Id,
			WOFId:    sp.WOFId,
			AltLabel: sp.AltLabel,
			// FIX ME
			// Bounds:   bounds,
		}

		rsp_ch <- c
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

	seen := make(map[int64]bool)

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

			sp_id := sp.Id
			wof_id := sp.WOFId

			mu.RLock()
			_, ok := seen[wof_id]
			mu.RUnlock()

			if ok {
				return
			}

			mu.Lock()
			seen[wof_id] = true
			mu.Unlock()

			cache_item, err := r.retrieveCache(ctx, sp)

			if err != nil {
				r.Logger.Error("Failed to retrieve cache for %s, %v", sp_id, err)
				return
			}

			s := cache_item.SPR

			for _, f := range filters {

				err = filter.FilterSPR(f, s)

				if err != nil {
					r.Logger.Debug("SKIP %s because filter error %s", sp_id, err)
					return
				}
			}

			geom := cache_item.Geometry

			contains := false

			switch geom.Type {
			case "Polygon":
				contains = geo.PolygonContainsCoord(geom.Polygon, c)
			case "MultiPolygon":
				contains = geo.MultiPolygonContainsCoord(geom.MultiPolygon, c)
			default:
				r.Logger.Warning("Geometry has unsupported geometry type '%s'", geom.Type)
			}

			if !contains {
				r.Logger.Debug("SKIP %s because does not contain coord (%v)", sp_id, c)
				return
			}

			rsp_ch <- s
		}(sp)
	}

	wg.Wait()
}

func (r *RTreeSpatialDatabase) setCache(ctx context.Context, f wof_geojson.Feature) error {

	s, err := f.SPR()

	if err != nil {
		return err
	}

	geom, err := geometry.GeometryForFeature(f)

	if err != nil {
		return err
	}

	alt_label := whosonfirst.AltLabel(f)

	wof_id := whosonfirst.Id(f)

	cache_key := fmt.Sprintf("%d:%s", wof_id, alt_label)

	cache_item := &RTreeCache{
		Geometry: geom,
		SPR:      s,
	}

	r.gocache.Set(cache_key, cache_item, -1)
	return nil
}

func (r *RTreeSpatialDatabase) retrieveCache(ctx context.Context, sp *RTreeSpatialIndex) (*RTreeCache, error) {

	cache_key := fmt.Sprintf("%d:%s", sp.WOFId, sp.AltLabel)

	cache_item, ok := r.gocache.Get(cache_key)

	if !ok {
		return nil, errors.New("Invalid cache ID")
	}

	return cache_item.(*RTreeCache), nil
}
