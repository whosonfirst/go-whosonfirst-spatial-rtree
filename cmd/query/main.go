package main

import (
	"context"
	"encoding/json"
	"fmt"
	// "github.com/sfomuseum/go-flags/multi"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-index"
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-rtree"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"io"
	"log"
	"net/url"
)

func main() {

	fs, err := flags.CommonFlags()

	if err != nil {
		log.Fatal(err)
	}

	err = flags.AppendIndexingFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	err = flags.AppendQueryFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	flags.Parse(fs)

	err = flags.ValidateCommonFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	err = flags.ValidateIndexingFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	err = flags.ValidateQueryFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	database_uri, _ := flags.StringVar(fs, "spatial-database-uri")
	// properties_uri, _ := flags.StringVar(fs, "properties-reader-uri")

	mode, _ := flags.StringVar(fs, "mode")

	latitude, _ := flags.Float64Var(fs, "latitude")
	longitude, _ := flags.Float64Var(fs, "longitude")

	geometries, _ := flags.StringVar(fs, "geometries")

	alt_geoms, _ := flags.MultiStringVar(fs, "alternate-geometry")
	is_current, _ := flags.MultiStringVar(fs, "is-current")
	is_ceased, _ := flags.MultiStringVar(fs, "is-ceased")
	is_deprecated, _ := flags.MultiStringVar(fs, "is-deprecated")
	is_superseded, _ := flags.MultiStringVar(fs, "is-superseded")
	is_superseding, _ := flags.MultiStringVar(fs, "is-superseding")

	pts, _ := flags.MultiStringVar(fs, "placetype")

	ctx := context.Background()

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", database_uri, err)
	}

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) error {

		f, err := feature.LoadFeatureFromReader(fh)

		if err != nil {
			return err
		}

		switch geometry.Type(f) {
		case "Polygon", "MultiPolygon":
			return db.IndexFeature(ctx, f)
		default:
			return nil
		}
	}

	i, err := index.NewIndexer(mode, cb)

	if err != nil {
		log.Fatal(err)
	}

	paths := fs.Args()

	err = i.Index(ctx, paths...)

	if err != nil {
		log.Fatal(err)
	}

	c, err := geo.NewCoordinate(longitude, latitude)

	if err != nil {
		log.Fatalf("Failed to create new coordinate, %v", err)
	}

	// START OF put me in a WithFlagSet(fs) function

	q := url.Values{}
	q.Set("geometries", geometries)

	for _, v := range alt_geoms {
		q.Add("alternate_geometry", v)
	}

	for _, v := range pts {
		q.Add("placetype", v)
	}

	for _, v := range is_current {
		q.Add("is_current", v)
	}

	for _, v := range is_ceased {
		q.Add("is_ceased", v)
	}

	for _, v := range is_deprecated {
		q.Add("is_deprecated", v)
	}

	for _, v := range is_superseded {
		q.Add("is_superseded", v)
	}

	for _, v := range is_superseding {
		q.Add("is_superseding", v)
	}

	f, err := filter.NewSPRFilterFromQuery(q)

	if err != nil {
		log.Fatalf("Failed to create SPR filter, %v", err)
	}

	// END OF put me in a WithFlagSet(fs) function

	r, err := db.PointInPolygon(ctx, c, f)

	if err != nil {
		log.Fatalf("Failed to query database with coord %v, %v", c, err)
	}

	enc, err := json.Marshal(r)

	if err != nil {
		log.Fatalf("Failed to marshal results, %v", err)
	}

	fmt.Println(string(enc))

	/*
		if *timings {

			for label, timings := range db.Timer.Timings {

				for _, tm := range timings {
					log.Printf("[%s] %s\n", label, tm)
				}
			}
		}
	*/
}
