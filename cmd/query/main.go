package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-index"
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-rtree"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/flags"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"io"
	"log"
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

	f, err := flags.NewSPRFilterFromFlagSet(fs)

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
