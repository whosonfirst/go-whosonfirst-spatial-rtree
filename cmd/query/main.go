package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-flags/multi"
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

	// flags.AppendQueryFlags(fs)

	latitude := fs.Float64("latitude", 0.0, "A valid latitude.")
	longitude := fs.Float64("longitude", 0.0, "A valid longitude.")

	geometries := fs.String("geometries", "all", "Valid options are: all, alt, default.")

	var props multi.MultiString
	fs.Var(&props, "properties", "One or more Who's On First properties to append to each result.")

	var pts multi.MultiString
	fs.Var(&pts, "placetype", "One or more place types to filter results by.")

	var alt_geoms multi.MultiString
	fs.Var(&alt_geoms, "alternate-geometry", "One or more alternate geometry labels (wof:alt_label) values to filter results by.")

	var is_current multi.MultiString
	fs.Var(&is_current, "is-current", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_ceased multi.MultiString
	fs.Var(&is_ceased, "is-ceased", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_deprecated multi.MultiString
	fs.Var(&is_deprecated, "is-deprecated", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_superseded multi.MultiString
	fs.Var(&is_superseded, "is-superseded", "One or more existential flags (-1, 0, 1) to filter results by.")

	var is_superseding multi.MultiString
	fs.Var(&is_superseding, "is-superseding", "One or more existential flags (-1, 0, 1) to filter results by.")

	flags.Parse(fs)

	err = flags.ValidateCommonFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	err = flags.ValidateIndexingFlags(fs)

	if err != nil {
		log.Fatal(err)
	}

	// flags.ValidateQueryFlags(fs)

	database_uri, _ := flags.StringVar(fs, "spatial-database-uri")
	// properties_uri, _ := flags.StringVar(fs, "properties-reader-uri")

	mode, _ := flags.StringVar(fs, "mode")

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

	paths := flag.Args()

	err = i.Index(ctx, paths...)

	if err != nil {
		log.Fatal(err)
	}

	c, err := geo.NewCoordinate(*longitude, *latitude)

	if err != nil {
		log.Fatalf("Failed to create new coordinate, %v", err)
	}

	// START OF put me in a WithFlagSet(fs) function

	q := url.Values{}
	q.Set("geometries", *geometries)

	for _, v := range alt_geoms {
		q.Add("alternate_geometry", v)
	}

	for _, v := range pts {
		q.Add("placetype", v)
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
