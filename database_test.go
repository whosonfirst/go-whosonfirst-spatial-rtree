package rtree

import (
	"context"
	_ "fmt"
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	"github.com/whosonfirst/go-whosonfirst-spatial/filter"
	"github.com/whosonfirst/go-whosonfirst-spatial/geo"
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
