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

## Tools

```
$> make cli
```

### query

## See also

* https://github.com/whosonfirst/go-whosonfirst-spatial
* https://github.com/dhconnelly/rtreego