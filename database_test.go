package rtree

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
	"io"
	"os"
	"strconv"
	"testing"
)

type Criteria struct {
	IsCurrent int64
	Latitude  float64
	Longitude float64
}

func TestSpatialDatabase(t *testing.T) {

	ctx := context.Background()

	database_uri := "rtree://?dsn=fixtures/sfomuseum-architecture.db"

	tests := map[int64]Criteria{
		1108712253: Criteria{Longitude: -71.120168, Latitude: 42.376015, IsCurrent: 1},   // Old Cambridge
		420561633:  Criteria{Longitude: -122.395268, Latitude: 37.794893, IsCurrent: 0},  // Superbowl City
		420780729:  Criteria{Longitude: -122.421529, Latitude: 37.743168, IsCurrent: -1}, // Liminal Zone of Deliciousness
	}

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

	err = database.IndexDatabaseWithIterator(ctx, db, "featurecollection://", "fixtures/microhoods.geojson")

	if err != nil {
		t.Fatalf("Failed to index spatial database, %v", err)
	}

	for expected, criteria := range tests {

		c, err := geo.NewCoordinate(criteria.Longitude, criteria.Latitude)

		if err != nil {
			t.Fatalf("Failed to create new coordinate, %v", err)
		}

		i, err := filter.NewSPRInputs()

		if err != nil {
			t.Fatalf("Failed to create SPR inputs, %v", err)
		}

		i.IsCurrent = []int64{criteria.IsCurrent}
		// i.Placetypes = []string{"microhood"}

		f, err := filter.NewSPRFilterFromInputs(i)

		if err != nil {
			t.Fatalf("Failed to create SPR filter from inputs, %v", err)
		}

		spr, err := db.PointInPolygon(ctx, c, f)

		if err != nil {
			t.Fatalf("Failed to perform point in polygon query, %v", err)
		}

		results := spr.Results()
		count := len(results)

		if count != 1 {
			t.Fatalf("Expected 1 result but got %d for '%d'", count, expected)
		}

		first := results[0]

		if first.Id() != strconv.FormatInt(expected, 10) {
			t.Fatalf("Expected %d but got %s", expected, first.Id())
		}
	}
}

func TestSpatialDatabaseRemoveFeature(t *testing.T) {

	ctx := context.Background()

	database_uri := "rtree://"

	db, err := database.NewSpatialDatabase(ctx, database_uri)

	if err != nil {
		t.Fatalf("Failed to create new spatial database, %v", err)
	}

	defer db.Close(ctx)

	id := 101737491
	lat := 46.852675
	lon := -71.330873

	test_data := fmt.Sprintf("fixtures/%d.geojson", id)

	fh, err := os.Open(test_data)

	if err != nil {
		t.Fatalf("Failed to open %s, %v", test_data, err)
	}

	defer fh.Close()

	body, err := io.ReadAll(fh)

	if err != nil {
		t.Fatalf("Failed to read %s, %v", test_data, err)
	}

	err = db.IndexFeature(ctx, body)

	if err != nil {
		t.Fatalf("Failed to index %s, %v", test_data, err)
	}

	c, err := geo.NewCoordinate(lon, lat)

	if err != nil {
		t.Fatalf("Failed to create new coordinate, %v", err)
	}

	spr, err := db.PointInPolygon(ctx, c)

	if err != nil {
		t.Fatalf("Failed to perform point in polygon query, %v", err)
	}

	results := spr.Results()
	count := len(results)

	if count != 1 {
		t.Fatalf("Expected 1 result but got %d", count)
	}

	err = db.RemoveFeature(ctx, "101737491")

	if err != nil {
		t.Fatalf("Failed to remove %s, %v", test_data, err)
	}

	spr, err = db.PointInPolygon(ctx, c)

	if err != nil {
		t.Fatalf("Failed to perform point in polygon query, %v", err)
	}

	results = spr.Results()
	count = len(results)

	if count != 0 {
		t.Fatalf("Expected 0 results but got %d", count)
	}
}
