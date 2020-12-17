# go-whosonfirst-spatial-rtree

## Interfaces

This package implements the following [go-whosonfirst-spatial](#) interfaces.

### spatial.SpatialDatabase

```
import (
	"github.com/whosonfirst/go-whosonfirst-spatial/database"
	_ "github.com/whosonfirst/go-whosonfirst-spatial-rtree"       
)

db, err := database.NewSpatialDatabase(ctx, "rtree://")
```

### Database URIs

The `go-whosonfirst-spatial-rtree` package is instantiated using a URI in the form of:

```
rtree://?{PARAMETERS}
```

Valid parameters include:

#### Parameters

| Name | Value | Required| Notes |
| --- | --- | --- | --- |
| strict | bool | N | |
| index_alt_files | bool | N | |

## Tools

```
$> make cli
```

### query

```
$> ./bin/query -h
Usage of ./bin/query:
  -database-uri string
    	... (default "rtree://?strict=false")
  -latitude float
    	... (default 37.616951)
  -longitude float
    	... (default -122.383747)
  -mode string
    	... (default "repo://")
```

For example:

```
$> ./bin/query \
	-database-uri 'rtree://?strict=false' \
	-latitude 37.616951 \
	-longitude -122.383747 \
	-mode repo:// \
	/usr/local/data/sfomuseum-data-architecture/ \

| jq | grep wof:name

17:08:24.974105 [query][index] ERROR 1159157931 failed indexing, (rtreego: improper distance). Strict mode is disabled, so skipping.
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "International Terminal",
      "wof:name": "International Terminal",
      "wof:name": "Central Terminal",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "Central Terminal",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "Terminal 2 Main Hall",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "Central Terminal",
      "wof:name": "Terminal 2",
      "wof:name": "Terminal 2 Main Hall",
      "wof:name": "Terminal 2",
      "wof:name": "Central Terminal",
      "wof:name": "Boarding Area D",
      "wof:name": "Boarding Area D",
      "wof:name": "Central Terminal",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
      "wof:name": "SFO Terminal Complex",
```

## See also

* https://github.com/whosonfirst/go-whosonfirst-spatial
* https://github.com/dhconnelly/rtreego