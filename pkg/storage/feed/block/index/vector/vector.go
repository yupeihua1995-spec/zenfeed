package vector

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/chewxy/math32"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/glidea/zenfeed/pkg/component"
	"github.com/glidea/zenfeed/pkg/model"
	"github.com/glidea/zenfeed/pkg/storage/feed/block/index"
	"github.com/glidea/zenfeed/pkg/telemetry"
	telemetrymodel "github.com/glidea/zenfeed/pkg/telemetry/model"
	"github.com/glidea/zenfeed/pkg/util/heap"
	timeutil "github.com/glidea/zenfeed/pkg/util/time"
	vectorutil "github.com/glidea/zenfeed/pkg/util/vector"
)

// --- Interface code block ---
type Index interface {
	component.Component
	index.Codec

	// Search returns feed IDs with vectors similar to the query vector.
	// If any chunk of the feed is similar to the query vector, the feed is returned.
	// And the score is the maximum score of the chunks.
	// But results may miss some similar feeds.
	// The result is a map of feed IDs to their similarity scores.
	// Score is in range [0, 1].
	Search(ctx context.Context, q []float32, threshold float32, limit int) (idWithScores map[uint64]float32, err error)

	// Add adds feed vectors to the index.
	// A feed may be split into multiple chunks, and each chunk is a vector.
	Add(ctx context.Context, id uint64, vectors [][]float32) (err error)
}

type Config struct {
	// M is the maximum number of neighbors to keep for each node.
	// The number of layer 0 is 2*M.
	M uint32

	// Ml is the level generation factor.
	Ml float32

	// EfSearch is the number of nodes to consider in the search phase.
	EfSearch uint32

	// EfConstruct is the number of nodes to consider in the construct phase.
	EfConstruct uint32
}

func (c *Config) Validate() error {
	if c.M <= 0 {
		c.M = 8
	}
	if c.Ml <= 0 {
		c.Ml = 0.2 // 1/ln(32).
	}
	if c.EfSearch <= 0 {
		c.EfSearch = 32
	}
	if c.EfConstruct <= 0 {
		c.EfConstruct = 64
	}

	return nil
}

type Dependencies struct{}

var Score = func(x, y [][]float32) (float32, error) {
	var max float32
	for i := range x {
		for j := range y {
			similarity, err := cosineSimilarity(x[i], y[j])
			if err != nil {
				return 0, errors.Wrap(err, "calculate similarity")
			}
			if similarity > max {
				max = similarity
			}
		}
	}

	return max, nil
}

var cosineSimilarity = func(x, y []float32) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.New("x and y must have the same length")
	}

	dp, xdp, ydp := float32(0), float32(0), float32(0)
	for i := range x {
		dp += x[i] * y[i]
		xdp += x[i] * x[i]
		ydp += y[i] * y[i]
	}

	return dp / math32.Sqrt(xdp*ydp), nil
}

// File.
var (
	headerMagicNumber = []byte{0x77, 0x79, 0x73, 0x20, 0x69, 0x73, 0x20,
		0x61, 0x77, 0x65, 0x73, 0x6f, 0x6d, 0x65, 0x00, 0x00}
	headerVersion = uint8(1)
)

// Metrics.
var (
	layerNodeCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: model.AppName,
			Subsystem: "vector_index",
			Name:      "layer_node_count",
			Help:      "Number of nodes in each layer of the vector index.",
		},
		[]string{telemetrymodel.KeyComponent, telemetrymodel.KeyComponentInstance, "layer"},
	)
)

// --- Factory code block ---
type Factory component.Factory[Index, Config, Dependencies]

func NewFactory(mockOn ...component.MockOption) Factory {
	if len(mockOn) > 0 {
		return component.FactoryFunc[Index, Config, Dependencies](
			func(instance string, config *Config, dependencies Dependencies) (Index, error) {
				m := &mockIndex{}
				component.MockOptions(mockOn).Apply(&m.Mock)

				return m, nil
			},
		)
	}

	return component.FactoryFunc[Index, Config, Dependencies](new)
}

func new(instance string, config *Config, dependencies Dependencies) (Index, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate config")
	}

	base := component.New(&component.BaseConfig[Config, Dependencies]{
		Name:         "FeedVectorIndex",
		Instance:     instance,
		Config:       config,
		Dependencies: dependencies,
	})
	m := make(map[uint64]*node, 32)
	layers := make([]*layer, 0, 8)
	layers = append(layers, &layer{
		level: 0,
		nodes: make([]uint64, 0),
		m:     &m,
	})

	return &idx{
		Base:   base,
		m:      m,
		layers: layers,
	}, nil
}

// --- Implementation code block ---
type idx struct {
	*component.Base[Config, Dependencies]

	m      map[uint64]*node
	layers []*layer
	mu     sync.RWMutex
}

func (idx *idx) Run() error {
	idx.MarkReady()

	return timeutil.Tick(idx.Context(), 30*time.Second, func() error {
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		for i, layer := range idx.layers {
			layerNodeCount.WithLabelValues(
				append(idx.TelemetryLabelsIDFields(), strconv.Itoa(i))...,
			).Set(float64(len(layer.nodes)))
		}

		return nil
	})
}

func (idx *idx) Close() error {
	// Close Run().
	if err := idx.Base.Close(); err != nil {
		return errors.Wrap(err, "closing base")
	}

	// Clean metrics.
	layerNodeCount.DeletePartialMatch(idx.TelemetryLabelsID())

	return nil
}

func (idx *idx) Search(
	ctx context.Context,
	q []float32,
	threshold float32,
	limit int,
) (idWithScores map[uint64]float32, err error) {
	ctx = telemetry.StartWith(ctx, append(idx.TelemetryLabels(), telemetrymodel.KeyOperation, "Search")...)
	defer func() { telemetry.End(ctx, err) }()
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if example := idx.layers[0].randomEntry(); example != nil && example.dimension() != len(q) {
		return nil, errors.New("vector dimension mismatch")
	}
	if err := validateVector(q); err != nil {
		return nil, err
	}

	// Find first entry node.
	var (
		entry      *node
		entryLevel int
	)
	for level := len(idx.layers) - 1; level >= 0; level-- {
		entry = idx.layers[level].randomEntry()
		if entry != nil {
			entryLevel = level

			break
		}
	}
	if entry == nil {
		return make(map[uint64]float32), nil
	}

	// Find entry node in the layer 0.
	for level := entryLevel; level > 0; level-- {
		similarNodes, err := idx.layers[level].search(ctx, [][]float32{q}, entry,
			idx.Config().EfSearch,
			1, // TopK, just find most similar one.
			0, // Threshold, no need to filter.
		)
		if err != nil {
			return nil, errors.Wrap(err, "search layer")
		}
		mostSimilarNode := similarNodes.best()
		entry = mostSimilarNode.node
	}

	// Search in the layer 0.
	similarNodes, err := idx.layers[0].search(ctx, [][]float32{q}, entry,
		idx.Config().EfSearch,
		uint32(limit),
		threshold, // Threshold.
	)
	if err != nil {
		return nil, errors.Wrap(err, "search layer")
	}

	idWithScores = make(map[uint64]float32, len(similarNodes))
	for _, node := range similarNodes {
		idWithScores[node.id] = node.score
	}

	return idWithScores, nil
}

func (idx *idx) Add(ctx context.Context, id uint64, vectors [][]float32) (err error) {
	ctx = telemetry.StartWith(ctx, append(idx.TelemetryLabels(), telemetrymodel.KeyOperation, "Add")...)
	defer func() { telemetry.End(ctx, err) }()
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if err := idx.validate(vectors); err != nil {
		return err
	}
	if _, exists := idx.m[id]; exists {
		return nil // Update is not supported.
	}

	insertLevel, maxLevel := idx.randomInsertLevel()
	newNode := &node{
		id:              id,
		vectors:         vectors,
		m:               &idx.m,
		friendsOnLayers: make([]map[uint64]float32, insertLevel+1),
	}
	shouldInsert := func(level int) bool {
		return level <= insertLevel
	}

	var entry *node
	for level := maxLevel; level >= 0; level-- {
		var skipLevel bool
		entry, skipLevel, err = idx.initializeEntryNodeForLevel(level, entry, newNode, shouldInsert)
		if err != nil {
			return errors.Wrap(err, "initialize entry node for level")
		}
		if skipLevel {
			continue
		}

		entry, err = idx.insertAndLinkAtLevel(ctx, level, newNode, entry, vectors, shouldInsert)
		if err != nil {
			idx.removeNode(newNode)

			return errors.Wrap(err, "insert and link at level")
		}
	}

	return nil
}

func (idx *idx) removeNode(target *node) {
	delete(idx.m, target.id)
	for _, layer := range idx.layers {
		for id := range target.friendsOnLayers[layer.level] {
			if friend := idx.m[id]; friend != nil && layer.level < len(friend.friendsOnLayers) {
				delete(friend.friendsOnLayers[layer.level], target.id)
			}
		}
		for i, id := range layer.nodes {
			if id == target.id {
				layer.nodes = append(layer.nodes[:i], layer.nodes[i+1:]...)

				break
			}
		}
	}
}

func (idx *idx) validate(vectors [][]float32) error {
	if len(vectors) == 0 || len(vectors[0]) == 0 {
		return errors.New("vectors are required")
	}

	dimension := len(vectors[0])
	for _, v := range vectors[1:] {
		if len(v) != dimension {
			return errors.New("vector dimension mismatch")
		}
	}
	if example := idx.layers[0].randomEntry(); example != nil && example.dimension() != dimension {
		return errors.New("vector dimension mismatch")
	}
	for _, v := range vectors {
		if err := validateVector(v); err != nil {
			return err
		}
	}

	return nil
}

func validateVector(vector []float32) error {
	var normSquared float32
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return errors.New("vector components must be finite")
		}
		normSquared += value * value
	}
	if normSquared == 0 || math.IsNaN(float64(normSquared)) || math.IsInf(float64(normSquared), 0) {
		return errors.New("vector norm must be finite and non-zero")
	}

	return nil
}

func (idx *idx) EncodeTo(ctx context.Context, w io.Writer) (err error) {
	ctx = telemetry.StartWith(ctx, append(idx.TelemetryLabels(), telemetrymodel.KeyOperation, "EncodeTo")...)
	defer func() { telemetry.End(ctx, err) }()
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Write header.
	if err := idx.writeHeader(w); err != nil {
		return errors.Wrap(err, "write header")
	}

	// Write nodes.
	if err := idx.writeNodes(w); err != nil {
		return errors.Wrap(err, "write nodes")
	}

	// Write layers.
	if err := idx.writeLayers(w); err != nil {
		return errors.Wrap(err, "write layers")
	}

	return nil
}

func (idx *idx) DecodeFrom(ctx context.Context, r io.Reader) (err error) {
	ctx = telemetry.StartWith(ctx, append(idx.TelemetryLabels(), telemetrymodel.KeyOperation, "DecodeFrom")...)
	defer func() { telemetry.End(ctx, err) }()
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Read header.
	c := idx.Config()
	if err := idx.readHeader(r, &c); err != nil {
		return errors.Wrap(err, "read header")
	}

	// Read nodes.
	if err := idx.readNodes(r); err != nil {
		return errors.Wrap(err, "read nodes")
	}

	// Read layers.
	if err := idx.readLayers(r); err != nil {
		return errors.Wrap(err, "read layers")
	}

	return nil
}

// initializeEntryNodeForLevel finds or initializes the entry node for a given level.
// It handles the case where the current level might be empty.
// If the level is empty and the node should be inserted, it adds the node to the index and layer.
// It returns the entry node for the search in this level, and a boolean indicating
// if the rest of the level processing should be skipped.
func (idx *idx) initializeEntryNodeForLevel(
	level int,
	currentEntry *node,
	newNode *node,
	shouldInsert func(level int) bool,
) (entry *node, skipLevel bool, err error) {
	if currentEntry != nil {
		return currentEntry, false, nil // Entry already found in higher levels.
	}

	// Try to find an entry point in the current layer.
	curLayerEntry := idx.layers[level].randomEntry()
	if curLayerEntry == nil { // Layer is empty.
		if shouldInsert(level) {
			// If this is an insertion level, add the new node directly.
			idx.m[newNode.id] = newNode
			idx.layers[level].nodes = append(idx.layers[level].nodes, newNode.id)
			// Friends map is initialized later in insertAndLinkAtLevel if needed,
			// but since the layer is empty, there are no friends to link yet.
			// Ensure the friends map for this level exists if it's an insertion level.
			if newNode.friendsOnLayers[level] == nil {
				newNode.friendsOnLayers[level] = make(map[uint64]float32)
			}
		}

		return nil, true, nil // Skip search and linking for this empty level.
	}

	return curLayerEntry, false, nil // Found an entry point for this level.
}

// insertAndLinkAtLevel performs the search for neighbors, inserts the new node if required,
// establishes friendships, and returns the entry node for the next lower level.
func (idx *idx) insertAndLinkAtLevel(
	ctx context.Context,
	level int,
	newNode *node,
	entry *node,
	vectors [][]float32,
	shouldInsert func(level int) bool,
) (nextEntry *node, err error) {
	// Determine the number of neighbors (M) for this level.
	m := idx.Config().M
	if level == 0 {
		m *= 2 // Layer 0 has double the connections.
	}

	// Search for the closest neighbors to the new node in the current level.
	similarNodes, err := idx.layers[level].search(ctx, vectors, entry,
		idx.Config().EfConstruct,
		m,
		0, // Threshold 0 to find candidates for construction.
	)
	if err != nil {
		return nil, errors.Wrap(err, "search layer")
	}

	// The best node found will be the entry point for the next level down.
	nextEntry = similarNodes.best().node

	if !shouldInsert(level) {
		return nextEntry, nil // Node is not inserted at this level.
	}

	// Insert the new node into the index and the current layer.
	idx.m[newNode.id] = newNode // Ensure node exists in the main map.
	idx.layers[level].nodes = append(idx.layers[level].nodes, newNode.id)
	newNode.friendsOnLayers[level] = make(map[uint64]float32, len(similarNodes)) // Initialize friends map for this level.

	// Establish bidirectional friendships with the found neighbors.
	for _, similarNode := range similarNodes {
		if err := newNode.makeFriend(similarNode.node, level, similarNode.score, int(m)); err != nil {
			return nil, errors.Wrap(err, "make friend for new node")
		}
		if err := similarNode.makeFriend(newNode, level, similarNode.score, int(m)); err != nil {
			// Log or handle potential error if the neighbor fails to add the new node.
			// Depending on requirements, this might or might not be a critical failure.
			// For now, wrap and return.
			return nil, errors.Wrap(err, "make friend for neighbor node")
		}
	}

	return nextEntry, nil
}

func (idx *idx) writeHeader(w io.Writer) error {
	if _, err := w.Write(headerMagicNumber); err != nil {
		return errors.Wrap(err, "write header magic number")
	}
	if _, err := w.Write([]byte{headerVersion}); err != nil {
		return errors.Wrap(err, "write header version")
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Config().M); err != nil {
		return errors.Wrap(err, "write header config M")
	}
	if err := binary.Write(w, binary.LittleEndian, math32.Float32bits(idx.Config().Ml)); err != nil {
		return errors.Wrap(err, "write header config Ml")
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Config().EfSearch); err != nil {
		return errors.Wrap(err, "write header config EfSearch")
	}
	if err := binary.Write(w, binary.LittleEndian, idx.Config().EfConstruct); err != nil {
		return errors.Wrap(err, "write header config EfConstruct")
	}

	return nil
}

func (idx *idx) writeNodes(w io.Writer) error {
	// Write node count.
	nodeCount := uint64(len(idx.m))
	if err := binary.Write(w, binary.LittleEndian, nodeCount); err != nil {
		return errors.Wrap(err, "write node count")
	}

	// Write each node.
	for _, node := range idx.m {
		if err := idx.writeNode(w, node); err != nil {
			return errors.Wrap(err, "write node")
		}
	}

	return nil
}

func (idx *idx) writeNode(w io.Writer, node *node) error {
	// Write node ID.
	if err := binary.Write(w, binary.LittleEndian, node.id); err != nil {
		return errors.Wrap(err, "write node id")
	}

	// Write vectors.
	chunks := uint32(len(node.vectors))
	if err := binary.Write(w, binary.LittleEndian, chunks); err != nil {
		return errors.Wrap(err, "write vector count")
	}
	dimension := uint32(len(node.vectors[0]))
	if err := binary.Write(w, binary.LittleEndian, dimension); err != nil {
		return errors.Wrap(err, "write vector dimension")
	}
	for _, vec := range node.vectors {
		if err := idx.writeNodeVector(w, vec); err != nil {
			return errors.Wrap(err, "write node vector")
		}
	}

	// Write node friends by layer.
	if err := idx.writeNodeFriends(w, node); err != nil {
		return errors.Wrap(err, "write node friends")
	}

	return nil
}

func (idx *idx) writeNodeVector(w io.Writer, vec []float32) error {
	// Quantize.
	quantized, min, scale := vectorutil.Quantize(vec)

	// Write the minimum and scale.
	if err := binary.Write(w, binary.LittleEndian, min); err != nil {
		return errors.Wrap(err, "write vector min value")
	}
	if err := binary.Write(w, binary.LittleEndian, scale); err != nil {
		return errors.Wrap(err, "write vector scale value")
	}

	// Write the quantized data.
	for _, q := range quantized {
		if err := binary.Write(w, binary.LittleEndian, q); err != nil {
			return errors.Wrap(err, "write quantized vector value")
		}
	}

	return nil
}

func (idx *idx) writeNodeFriends(w io.Writer, node *node) error {
	// Write layer count.
	if err := binary.Write(w, binary.LittleEndian, uint32(len(node.friendsOnLayers))); err != nil {
		return errors.Wrap(err, "write layer count")
	}

	// Write friends for each layer.
	for _, friends := range node.friendsOnLayers {
		// Write friend count.
		if err := binary.Write(w, binary.LittleEndian, uint32(len(friends))); err != nil {
			return errors.Wrap(err, "write friend count")
		}

		// Write each friend.
		for friendID := range friends {
			if err := binary.Write(w, binary.LittleEndian, friendID); err != nil {
				return errors.Wrap(err, "write friend id")
			}
			if err := binary.Write(w, binary.LittleEndian, friends[friendID]); err != nil {
				return errors.Wrap(err, "write friend score")
			}
		}
	}

	return nil
}

func (idx *idx) writeLayers(w io.Writer) error {
	// Write layer count.
	layerCount := uint32(len(idx.layers))
	if err := binary.Write(w, binary.LittleEndian, layerCount); err != nil {
		return errors.Wrap(err, "write total layer count")
	}

	// Write each layer.
	for _, layer := range idx.layers {
		// Write node count.
		nodeCount := uint32(len(layer.nodes))
		if err := binary.Write(w, binary.LittleEndian, nodeCount); err != nil {
			return errors.Wrap(err, "write layer node count")
		}

		// Write each node ID.
		for _, id := range layer.nodes {
			if err := binary.Write(w, binary.LittleEndian, id); err != nil {
				return errors.Wrap(err, "write layer node id")
			}
		}
	}

	return nil
}

func (idx *idx) readHeader(r io.Reader, config **Config) error {
	// Read magic number.
	magicNumber := make([]byte, len(headerMagicNumber))
	if _, err := io.ReadFull(r, magicNumber); err != nil {
		return errors.Wrap(err, "read header magic number")
	}
	if !bytes.Equal(magicNumber, headerMagicNumber) {
		return errors.New("invalid magic number")
	}

	// Read version.
	versionByte := make([]byte, 1)
	if _, err := io.ReadFull(r, versionByte); err != nil {
		return errors.Wrap(err, "read header version")
	}
	if versionByte[0] != headerVersion {
		return errors.New("invalid version")
	}

	// Read config.
	*config = &Config{}
	if err := binary.Read(r, binary.LittleEndian, &(*config).M); err != nil {
		return errors.Wrap(err, "read header config M")
	}
	var mlBits uint32
	if err := binary.Read(r, binary.LittleEndian, &mlBits); err != nil {
		return errors.Wrap(err, "read header config Ml")
	}
	(*config).Ml = math32.Float32frombits(mlBits)
	if err := binary.Read(r, binary.LittleEndian, &(*config).EfSearch); err != nil {
		return errors.Wrap(err, "read header config EfSearch")
	}
	if err := binary.Read(r, binary.LittleEndian, &(*config).EfConstruct); err != nil {
		return errors.Wrap(err, "read header config EfConstruct")
	}

	return nil
}

func (idx *idx) readNodes(r io.Reader) error {
	// Read node count.
	var nodeCount uint64
	if err := binary.Read(r, binary.LittleEndian, &nodeCount); err != nil {
		return errors.Wrap(err, "read node count")
	}
	idx.m = make(map[uint64]*node, nodeCount)

	// Read each node.
	for range nodeCount {
		if err := idx.readNode(r); err != nil {
			return errors.Wrap(err, "read node")
		}
	}

	return nil
}

func (idx *idx) readNode(r io.Reader) error {
	// Read node ID.
	var id uint64
	if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
		return errors.Wrap(err, "read node id")
	}

	// Read vectors metadata.
	var chunks, dimension uint32
	if err := binary.Read(r, binary.LittleEndian, &chunks); err != nil {
		return errors.Wrap(err, "read vector count")
	}
	if err := binary.Read(r, binary.LittleEndian, &dimension); err != nil {
		return errors.Wrap(err, "read vector dimension")
	}

	// Read vectors.
	vectors := make([][]float32, chunks)
	for j := range chunks {
		vec, err := idx.readQuantizedVector(r, dimension)
		if err != nil {
			return errors.Wrap(err, "read quantized vector")
		}
		vectors[j] = vec
	}

	// Create node.
	newNode := &node{
		id:      id,
		vectors: vectors,
		m:       &idx.m,
	}

	// Read friends.
	if err := idx.readNodeFriends(r, newNode); err != nil {
		return errors.Wrap(err, "read node friends")
	}

	idx.m[id] = newNode

	return nil
}

func (idx *idx) readQuantizedVector(r io.Reader, dimension uint32) ([]float32, error) {
	var min, scale float32
	if err := binary.Read(r, binary.LittleEndian, &min); err != nil {
		return nil, errors.Wrap(err, "read vector min value")
	}
	if err := binary.Read(r, binary.LittleEndian, &scale); err != nil {
		return nil, errors.Wrap(err, "read vector scale value")
	}

	// Read the quantized data.
	quantized := make([]int8, dimension)
	for i := range dimension {
		if err := binary.Read(r, binary.LittleEndian, &quantized[i]); err != nil {
			return nil, errors.Wrap(err, "read quantized vector value")
		}
	}

	// Dequantize.
	return vectorutil.Dequantize(quantized, min, scale), nil
}

func (idx *idx) readNodeFriends(r io.Reader, node *node) error {
	// Read layer count.
	var layerCount uint32
	if err := binary.Read(r, binary.LittleEndian, &layerCount); err != nil {
		return errors.Wrap(err, "read layer count")
	}

	node.friendsOnLayers = make([]map[uint64]float32, layerCount)

	// Read friends for each layer.
	for j := range layerCount {
		// Read friend count.
		var friendCount uint32
		if err := binary.Read(r, binary.LittleEndian, &friendCount); err != nil {
			return errors.Wrap(err, "read friend count")
		}

		// Read each friend ID.
		friends := make(map[uint64]float32, friendCount)
		for range friendCount {
			var friendID uint64
			if err := binary.Read(r, binary.LittleEndian, &friendID); err != nil {
				return errors.Wrap(err, "read friend id")
			}
			var score float32
			if err := binary.Read(r, binary.LittleEndian, &score); err != nil {
				return errors.Wrap(err, "read friend score")
			}
			friends[friendID] = score
		}
		node.friendsOnLayers[j] = friends
	}

	return nil
}

func (idx *idx) readLayers(r io.Reader) error {
	// Read layer count.
	var totalLayerCount uint32
	if err := binary.Read(r, binary.LittleEndian, &totalLayerCount); err != nil {
		return errors.Wrap(err, "read total layer count")
	}
	idx.layers = make([]*layer, totalLayerCount)

	// Read each layer.
	for i := range totalLayerCount {
		// Read node count.
		var nodeCount uint32
		if err := binary.Read(r, binary.LittleEndian, &nodeCount); err != nil {
			return errors.Wrap(err, "read layer node count")
		}

		// Create layer.
		layer := &layer{
			level: int(i),
			nodes: make([]uint64, nodeCount),
			m:     &idx.m,
		}
		idx.layers[i] = layer

		// Read each node ID.
		for j := range nodeCount {
			if err := binary.Read(r, binary.LittleEndian, &layer.nodes[j]); err != nil {
				return errors.Wrap(err, "read layer node id")
			}
		}
	}

	return nil
}

func (idx *idx) maxLevel() int {
	var max int
	switch {
	case len(idx.layers[0].nodes) == 0:
		max = 1
	default:
		l := math.Log(float64(len(idx.layers[0].nodes)))
		l /= math.Log(1 / float64(idx.Config().Ml))
		max = int(math.Round(l)) + 1 // ⌊log1/ml (N)⌋+1.
	}

	idx.ensureLayerSpace(max)

	return max - 1 // Level index starts from 0.
}

func (idx *idx) ensureLayerSpace(max int) {
	if max > len(idx.layers) {
		// Grow the layers.
		for i := len(idx.layers); i < max; i++ {
			idx.layers = append(idx.layers, &layer{
				level: i,
				nodes: make([]uint64, 0),
				m:     &idx.m,
			})
		}
	}
}

func (idx *idx) randomInsertLevel() (inserted, max int) {
	max = idx.maxLevel()
	for level := range max {
		if rand.Float32() > idx.Config().Ml {
			return level, max
		}
	}

	return max, max
}

type layer struct {
	m *map[uint64]*node // No marshal.

	level int
	nodes []uint64 // Just ID, avoid cycle reference to easy marshal.
}

func (l *layer) randomEntry() *node {
	if len(l.nodes) == 0 {
		return nil
	}

	id := l.nodes[rand.IntN(len(l.nodes))]

	return (*l.m)[id]
}

func (l *layer) search(
	ctx context.Context,
	q [][]float32,
	entry *node,
	ef, topK uint32,
	threshold float32,
) (nodes similarNodes, err error) {
	ctx = telemetry.StartWith(ctx, telemetrymodel.KeyOperation, "search")
	defer func() { telemetry.End(ctx, err) }()

	// Prepare.
	var (
		candidates = heap.New(make(similarNodes, 0, ef), func(a, b *similarNode) bool {
			return a.score > b.score
		})
		result = heap.New(make(similarNodes, 0, topK), func(a, b *similarNode) bool {
			return a.score < b.score
		})
		visited = make(map[uint64]bool)
	)

	entryNode, err := l.warpSearchEntryNode(q, entry)
	if err != nil {
		return nil, errors.Wrap(err, "warp search entry node")
	}

	// Add the entry node to the candidate set and result set.
	candidates.Push(entryNode)
	visited[entry.id] = true

	// Greedy search.
	for candidates.Len() > 0 {
		// Get the highest score candidate node.
		current := candidates.Pop()

		// Add to the result.
		if current.score >= threshold {
			result.TryEvictPush(current)
		}

		// Check all friends of the current node.
		hasBetter := false
		for id := range current.friendsOnLayers[l.level] {
			friend := (*l.m)[id]
			friendHasBetter, err := l.processFriend(q, friend, visited, candidates, result)
			if err != nil {
				return nil, errors.Wrap(err, "process friend")
			}
			if friendHasBetter {
				hasBetter = true
			}
		}

		// Consider break early.
		// Result full, and no better score for now.
		if !hasBetter && result.Len() >= result.Cap() {
			break
		}
	}

	return result.Slice(), nil
}

func (l *layer) warpSearchEntryNode(
	q [][]float32,
	entry *node,
) (*similarNode, error) {
	score, err := Score(q, entry.vectors)
	if err != nil {
		return nil, errors.Wrap(err, "calculate entry score")
	}

	return &similarNode{node: entry, score: score}, nil
}

// processFriend handles the processing of a single friend node during the search.
// It calculates the score, updates the visited set, and manages the candidate/result heaps.
func (l *layer) processFriend(
	q [][]float32,
	friend *node,
	visited map[uint64]bool,
	candidates *heap.Heap[*similarNode],
	result *heap.Heap[*similarNode],
) (hasBetter bool, err error) {
	if visited[friend.id] {
		return false, nil // Already visited.
	}
	visited[friend.id] = true

	// Calculate the score of the friend node.
	friendScore, err := Score(q, friend.vectors)
	if err != nil {
		return false, errors.Wrap(err, "calculate friend score")
	}

	// Check if this friend could potentially improve the result.
	// hasBetter means friend's score is better than the worst score in the current result heap.
	if result.Len() > 0 && friendScore > result.Peek().score {
		hasBetter = true
	}
	friendNode := &similarNode{node: friend, score: friendScore}

	// Add to the candidate set.
	if candidates.Len() >= candidates.Cap() {
		candidates.PopLast() // We pop first to avoid the heap growing.
	}
	candidates.Push(friendNode)

	return hasBetter, nil
}

type node struct {
	id      uint64
	vectors [][]float32

	m               *map[uint64]*node    // No marshal.
	friendsOnLayers []map[uint64]float32 // Avoid cycle reference to easy marshal.
}

func (n *node) dimension() int {
	return len(n.vectors[0])
}

func (n *node) makeFriend(
	friend *node,
	layer int,
	score float32,
	max int,
) (err error) {
	if n.friendsOnLayers[layer] == nil {
		n.friendsOnLayers[layer] = make(map[uint64]float32, max)
	}
	friends := n.friendsOnLayers[layer]
	friends[friend.id] = score

	return n.tryRemoveFriend(layer, max)
}

func (n *node) tryRemoveFriend(layer, max int) (err error) {
	friends := n.friendsOnLayers[layer]
	if len(friends) <= max {
		return nil
	}

	// Find the least similar friend a.
	a, err := n.leastSimilarFriend(layer)
	if err != nil {
		return errors.Wrap(err, "find least similar friend")
	}

	// Keep friendship with a if a has few friends.
	// TODO: it will lead to super node, may not be good.
	friendsA := a.friendsOnLayers[layer]
	if len(friendsA) < max/4+1 {
		return nil
	}

	// Delete the least similar friend a.
	delete(friends, n.id)
	delete(friendsA, a.id)

	// Remake one friend for a, by friend's friends.
	return a.tryRemakeFriend(layer, max)
}

func (n *node) tryRemakeFriend(layer, max int) (err error) {
	friends := n.friendsOnLayers[layer]
	if len(friends) > max/2+1 {
		return nil // No bad, no need to remake.
	}

	// Iterate through current node's friends.
	maxTryCount := max/4 + 1
	tryCount := 0
	for friendID := range friends {
		friend := (*n.m)[friendID]
		if tryCount >= maxTryCount {
			break
		}
		tryCount++

		// Check social circle similarity.
		if newFriend := n.findNewFriendFromFriendOfFriend(friend, friends, layer, max); newFriend {
			return nil // Successfully remade one friendship.
		}
	}

	return nil
}

func (n *node) findNewFriendFromFriendOfFriend(
	friend *node,
	friends map[uint64]float32,
	layer, max int,
) bool {
	similarCircleThreshold := 0.8
	minSampleCount := 0.5
	friendOfFriendIsMayFriends := 0.0
	total := 0.0
	maxPreference := 3
	if len(friends) < 5 {
		maxPreference = 1
	}
	tryPreferLessFriends := 0

	// Iterate through friend's friends.
	for friendOfFriendID := range friend.friendsOnLayers[layer] {
		friendOfFriend := (*n.m)[friendOfFriendID]
		if friendOfFriend.id == n.id {
			continue // Skip self.
		}

		total++

		// Check if already a friend.
		if _, ok := friends[friendOfFriend.id]; ok {
			friendOfFriendIsMayFriends++
			// If social circle is highly similar, might already know all common friends.
			if total >= minSampleCount && friendOfFriendIsMayFriends/total >= similarCircleThreshold {
				return false
			}

			continue // Already a friend.
		}

		// Prefer nodes with fewer friends.
		if len(friendOfFriend.friendsOnLayers[layer]) >= max && tryPreferLessFriends < maxPreference {
			tryPreferLessFriends++

			continue // Skip nodes with full friends.
		}

		// Calculate similarity and establish friendship.
		score, err := Score(n.vectors, friendOfFriend.vectors)
		if err != nil {
			return false // Error calculating score.
		}

		n.friendsOnLayers[layer][friendOfFriend.id] = score
		friendOfFriend.friendsOnLayers[layer][n.id] = score

		return true // Remake one friendship only.
	}

	return false
}

func (n *node) leastSimilarFriend(layer int) (friend *node, err error) {
	var min float32
	for id, s := range n.friendsOnLayers[layer] {
		f := (*n.m)[id]
		if friend == nil || s < min {
			friend = f
			min = s
		}
	}

	return friend, nil
}

type similarNodes []*similarNode

func (s similarNodes) best() *similarNode {
	best := s[0]
	for _, node := range s {
		if node.score > best.score {
			best = node
		}
	}

	return best
}

type similarNode struct {
	*node
	score float32
}

type mockIndex struct {
	component.Mock
}

func (m *mockIndex) Search(ctx context.Context, q []float32, threshold float32, limit int) (map[uint64]float32, error) {
	args := m.Called(ctx, q, threshold, limit)

	return args.Get(0).(map[uint64]float32), args.Error(1)
}

func (m *mockIndex) Add(ctx context.Context, id uint64, vectors [][]float32) error {
	args := m.Called(ctx, id, vectors)

	return args.Error(0)
}

func (m *mockIndex) EncodeTo(ctx context.Context, w io.Writer) error {
	args := m.Called(ctx, w)

	return args.Error(0)
}

func (m *mockIndex) DecodeFrom(ctx context.Context, r io.Reader) error {
	args := m.Called(ctx, r)

	return args.Error(0)
}
