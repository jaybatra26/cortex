package chunk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/chunk/cache"
)

// StoreLimits helps get Limits specific to Queries for Stores
type StoreLimits interface {
	MaxChunksPerQuery(userID string) int
	MaxQueryLength(userID string) time.Duration
}

type CacheGenNumLoader interface {
	GetStoreCacheGenNumber(userID string) string
}

// Store for chunks.
type Store interface {
	Put(ctx context.Context, chunks []Chunk) error
	PutOne(ctx context.Context, from, through model.Time, chunk Chunk) error
	Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]Chunk, error)
	// GetChunkRefs returns the un-loaded chunks and the fetchers to be used to load them. You can load each slice of chunks ([]Chunk),
	// using the corresponding Fetcher (fetchers[i].FetchChunks(ctx, chunks[i], ...)
	GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]Chunk, []*Fetcher, error)
	LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string) ([]string, error)
	LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error)

	// DeleteChunk deletes a chunks index entry and then deletes the actual chunk from chunk storage.
	// It takes care of chunks which are deleting partially by creating and inserting a new chunk first and then deleting the original chunk
	DeleteChunk(ctx context.Context, from, through model.Time, userID, chunkID string, metric labels.Labels, partiallyDeletedInterval *model.Interval) error
	// DeleteSeriesIDs is only relevant for SeriesStore.
	DeleteSeriesIDs(ctx context.Context, from, through model.Time, userID string, metric labels.Labels) error
	Stop()
}

// CompositeStore is a Store which delegates to various stores depending
// on when they were activated.
type CompositeStore struct {
	compositeStore
}

type compositeStore struct {
	cacheGenNumLoader CacheGenNumLoader
	stores            []compositeStoreEntry
}

type compositeStoreEntry struct {
	start model.Time
	Store
}

// NewCompositeStore creates a new Store which delegates to different stores depending
// on time.
func NewCompositeStore(cacheGenNumLoader CacheGenNumLoader) CompositeStore {
	return CompositeStore{compositeStore{cacheGenNumLoader: cacheGenNumLoader}}
}

// AddPeriod adds the configuration for a period of time to the CompositeStore
func (c *CompositeStore) AddPeriod(storeCfg StoreConfig, cfg PeriodConfig, index IndexClient, chunks Client, limits StoreLimits, chunksCache, writeDedupeCache cache.Cache) error {
	schema, err := cfg.CreateSchema()
	if err != nil {
		return err
	}

	return c.addSchema(storeCfg, schema, cfg.From.Time, index, chunks, limits, chunksCache, writeDedupeCache)
}

func (c *CompositeStore) addSchema(storeCfg StoreConfig, schema BaseSchema, start model.Time, index IndexClient, chunks Client, limits StoreLimits, chunksCache, writeDedupeCache cache.Cache) error {
	var (
		err   error
		store Store
	)

	switch s := schema.(type) {
	case SeriesStoreSchema:
		store, err = newSeriesStore(storeCfg, s, index, chunks, limits, chunksCache, writeDedupeCache)
	case StoreSchema:
		store, err = newStore(storeCfg, s, index, chunks, limits, chunksCache)
	default:
		err = errors.New("invalid schema type")
	}
	if err != nil {
		return err
	}
	c.stores = append(c.stores, compositeStoreEntry{start: start, Store: store})
	return nil
}

func (c compositeStore) Put(ctx context.Context, chunks []Chunk) error {
	for _, chunk := range chunks {
		err := c.forStores(ctx, chunk.UserID, chunk.From, chunk.Through, func(innerCtx context.Context, from, through model.Time, store Store) error {
			return store.PutOne(innerCtx, from, through, chunk)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c compositeStore) PutOne(ctx context.Context, from, through model.Time, chunk Chunk) error {
	return c.forStores(ctx, chunk.UserID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		return store.PutOne(innerCtx, from, through, chunk)
	})
}
func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
func Helper2(chunks []Chunk) {
	// count: every time we see a label name, if we have not seen this label value before, add it to a list
	type labelMap map[string]string

	//  := make(map[string][]labelMap)
	//label name: [label value1, label value2.....]
	countMap := make(map[string] /*query name*/ map[string] /*label name*/ map[string] /*label value*/ bool)
	for _, ch_ := range chunks {
		metrics := ch_.Metric

		metricName := ch_.Metric.Get(labels.MetricName)
		for _, label_ := range metrics {
			keyData, ok := countMap[metricName]
			if ok {
				labelName, name_ok := keyData[label_.Name]
				if name_ok {
					_, value_ok := labelName[label_.Value]
					if !value_ok {
						labelName[label_.Name] = true
					}
				} else {
					keyData[label_.Name] = make(map[string]bool)
					keyData[label_.Name][label_.Value] = true
				}
			} else {
				countMap[metricName] = make(map[string]map[string]bool)
				countMap[metricName][label_.Name] = make(map[string]bool)
				countMap[metricName][label_.Name][label_.Value] = true
			}
		}
	}
	type leastlabel struct {
		metricName string
		key        string
		len        int
	}
	leastlabels := []leastlabel{}
	for metricName, element := range countMap {
		for key, el2 := range element {
			llabel_ := leastlabel{metricName, key, len(el2)}
			leastlabels = append(leastlabels, llabel_)
		}
	}

	sort.SliceStable(leastlabels, func(i, j int) bool {
		return leastlabels[i].len < leastlabels[j].len
	})
	fmt.Println(leastlabels)
}

func (c compositeStore) Get(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([]Chunk, error) {
	var results []Chunk
	err := c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		chunks, err := store.Get(innerCtx, userID, from, through, matchers...)
		if err != nil {
			return err
		}
		Helper2(chunks)
		results = append(results, chunks...)
		return nil
	})
	return results, err
}

// LabelValuesForMetricName retrieves all label values for a single label name and metric name.
func (c compositeStore) LabelValuesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string, labelName string) ([]string, error) {
	var result UniqueStrings
	err := c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		labelValues, err := store.LabelValuesForMetricName(innerCtx, userID, from, through, metricName, labelName)
		if err != nil {
			return err
		}
		result.Add(labelValues...)
		return nil
	})
	return result.Strings(), err
}

// LabelNamesForMetricName retrieves all label names for a metric name.
func (c compositeStore) LabelNamesForMetricName(ctx context.Context, userID string, from, through model.Time, metricName string) ([]string, error) {
	var result UniqueStrings
	err := c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		labelNames, err := store.LabelNamesForMetricName(innerCtx, userID, from, through, metricName)
		if err != nil {
			return err
		}
		result.Add(labelNames...)
		return nil
	})
	return result.Strings(), err
}

func (c compositeStore) GetChunkRefs(ctx context.Context, userID string, from, through model.Time, matchers ...*labels.Matcher) ([][]Chunk, []*Fetcher, error) {
	chunkIDs := [][]Chunk{}
	fetchers := []*Fetcher{}
	err := c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		ids, fetcher, err := store.GetChunkRefs(innerCtx, userID, from, through, matchers...)
		if err != nil {
			return err
		}

		// Skip it if there are no chunks.
		if len(ids) == 0 {
			return nil
		}

		chunkIDs = append(chunkIDs, ids...)
		fetchers = append(fetchers, fetcher...)
		return nil
	})
	return chunkIDs, fetchers, err
}

// DeleteSeriesIDs deletes series IDs from index in series store
func (c CompositeStore) DeleteSeriesIDs(ctx context.Context, from, through model.Time, userID string, metric labels.Labels) error {
	return c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		return store.DeleteSeriesIDs(innerCtx, from, through, userID, metric)
	})
}

// DeleteChunk deletes a chunks index entry and then deletes the actual chunk from chunk storage.
// It takes care of chunks which are deleting partially by creating and inserting a new chunk first and then deleting the original chunk
func (c CompositeStore) DeleteChunk(ctx context.Context, from, through model.Time, userID, chunkID string, metric labels.Labels, partiallyDeletedInterval *model.Interval) error {
	return c.forStores(ctx, userID, from, through, func(innerCtx context.Context, from, through model.Time, store Store) error {
		return store.DeleteChunk(innerCtx, from, through, userID, chunkID, metric, partiallyDeletedInterval)
	})
}

func (c compositeStore) Stop() {
	for _, store := range c.stores {
		store.Stop()
	}
}

func (c compositeStore) forStores(ctx context.Context, userID string, from, through model.Time, callback func(innerCtx context.Context, from, through model.Time, store Store) error) error {
	if len(c.stores) == 0 {
		return nil
	}

	ctx = c.injectCacheGen(ctx, userID)

	// first, find the schema with the highest start _before or at_ from
	i := sort.Search(len(c.stores), func(i int) bool {
		return c.stores[i].start > from
	})
	if i > 0 {
		i--
	} else {
		// This could happen if we get passed a sample from before 1970.
		i = 0
		from = c.stores[0].start
	}

	// next, find the schema with the lowest start _after_ through
	j := sort.Search(len(c.stores), func(j int) bool {
		return c.stores[j].start > through
	})

	min := func(a, b model.Time) model.Time {
		if a < b {
			return a
		}
		return b
	}

	start := from
	for ; i < j; i++ {
		nextSchemaStarts := model.Latest
		if i+1 < len(c.stores) {
			nextSchemaStarts = c.stores[i+1].start
		}

		// If the next schema starts at the same time as this one,
		// skip this one.
		if nextSchemaStarts == c.stores[i].start {
			continue
		}

		end := min(through, nextSchemaStarts-1)
		err := callback(ctx, start, end, c.stores[i].Store)
		if err != nil {
			return err
		}

		start = nextSchemaStarts
	}

	return nil
}

func (c compositeStore) injectCacheGen(ctx context.Context, userID string) context.Context {
	if c.cacheGenNumLoader == nil {
		return ctx
	}

	return cache.InjectCacheGenNumber(ctx, c.cacheGenNumLoader.GetStoreCacheGenNumber(userID))
}
