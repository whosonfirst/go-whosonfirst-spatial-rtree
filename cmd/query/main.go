package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-index"
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-rtree"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"io"
	"log"
)

func main() {

	database_uri := flag.String("database-uri", "rtree://?strict=false", "...")
	latitude := flag.Float64("latitude", 37.616951, "...")
	longitude := flag.Float64("longitude", -122.383747, "...")

	// TBD...
	// timings := flag.Bool("timings", false, "...")

	mode := flag.String("mode", "repo://", "...")

	flag.Parse()

	ctx := context.Background()

	db, err := database.NewSpatialDatabase(ctx, *database_uri)

	if err != nil {
		log.Fatalf("Failed to create database for '%s', %v", *database_uri, err)
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

	i, err := index.NewIndexer(*mode, cb)

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

	f, err := filter.NewSPRFilter()

	if err != nil {
		log.Fatalf("Failed to create SPR filter, %v", err)
	}

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
