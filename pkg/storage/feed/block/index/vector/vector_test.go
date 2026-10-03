package vector

import (
	"bytes"
	"context"
	"math"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/glidea/zenfeed/pkg/test"
)

func TestSearch(t *testing.T) {
	RegisterTestingT(t)

	type givenDetail struct {
		vectors map[uint64][][]float32
	}
	type whenDetail struct {
		q         []float32
		threshold float32
		limit     int
	}
	type thenExpected struct {
		idWithScores map[uint64]float32
		err          string
	}

	tests := []test.Case[givenDetail, whenDetail, thenExpected]{
		{
			Scenario: "Search for similar vectors",
			Given:    "An index with some vectors",
			When:     "Searching for a vector with a threshold",
			Then:     "Should return IDs of similar vectors with scores",
			GivenDetail: givenDetail{
				vectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
					2: {{0.8, 1.0, 0.0}},
					3: {{0.8, 0.1, 0.1} /*0.9847*/, {0.7, 0.1, 0.9} /*0.6116*/},
				},
			},
			WhenDetail: whenDetail{
				q:         []float32{1.0, 0.0, 0.0},
				threshold: 0.9,
				limit:     5,
			},
			ThenExpected: thenExpected{
				idWithScores: map[uint64]float32{
					1: 1.0,
					3: 0.9847,
				},
			},
		},
		{
			Scenario: "Search for similar vectors with strict limit",
			Given:    "An index with some vectors",
			When:     "Searching for a vector with a strict limit",
			Then:     "Should return IDs of similar vectors with scores",
			GivenDetail: givenDetail{
				vectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
					2: {{0.8, 1.0, 0.0}},
					3: {{0.8, 0.1, 0.1} /*0.9847*/, {0.7, 0.1, 0.9} /*0.6116*/},
				},
			},
			WhenDetail: whenDetail{
				q:         []float32{1.0, 0.0, 0.0},
				threshold: 0.9,
				limit:     1,
			},
			ThenExpected: thenExpected{
				idWithScores: map[uint64]float32{
					1: 1.0,
				},
			},
		},
		{
			Scenario: "Search with dimension mismatch",
			Given:    "An index with some vectors",
			When:     "Searching for a vector with different dimension",
			Then:     "Should return an error",
			GivenDetail: givenDetail{
				vectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
				},
			},
			WhenDetail: whenDetail{
				q:         []float32{1.0, 0.0}, // Different dimension.
				threshold: 0.8,
				limit:     10,
			},
			ThenExpected: thenExpected{
				err: "vector dimension mismatch",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Scenario, func(t *testing.T) {
			// Given.
			idx, err := NewFactory().New("test", &Config{}, Dependencies{})
			Expect(err).NotTo(HaveOccurred())
			for id, vectors := range tt.GivenDetail.vectors {
				err := idx.Add(context.Background(), id, vectors)
				Expect(err).NotTo(HaveOccurred())
			}

			// When.
			idWithScores, err := idx.Search(context.Background(), tt.WhenDetail.q, tt.WhenDetail.threshold, tt.WhenDetail.limit)

			// Then.
			if tt.ThenExpected.err != "" {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(tt.ThenExpected.err))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(idWithScores).To(HaveLen(len(tt.ThenExpected.idWithScores)))
				for id, score := range tt.ThenExpected.idWithScores {
					Expect(idWithScores).To(HaveKey(id))
					Expect(idWithScores[id]).To(BeNumerically("~", score, 0.01))
				}
			}
		})
	}
}

func TestAdd(t *testing.T) {
	RegisterTestingT(t)

	type givenDetail struct {
		existingVectors map[uint64][][]float32
	}
	type whenDetail struct {
		id      uint64
		vectors [][]float32
	}
	type thenExpected struct {
		err           string
		nodeExists    bool
		layersContain bool
	}

	tests := []test.Case[givenDetail, whenDetail, thenExpected]{
		{
			Scenario: "Add a vector to an empty index",
			Given:    "An empty vector index",
			When:     "Adding a vector",
			Then:     "Should add the vector and update layers",
			GivenDetail: givenDetail{
				existingVectors: map[uint64][][]float32{},
			},
			WhenDetail: whenDetail{
				id:      1,
				vectors: [][]float32{{1.0, 0.0, 0.0}},
			},
			ThenExpected: thenExpected{
				nodeExists:    true,
				layersContain: true,
			},
		},
		{
			Scenario: "Add multiple vectors",
			Given:    "An index with existing vectors",
			When:     "Adding another vector",
			Then:     "Should add the vector and update layers",
			GivenDetail: givenDetail{
				existingVectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
				},
			},
			WhenDetail: whenDetail{
				id:      2,
				vectors: [][]float32{{0.0, 1.0, 0.0}},
			},
			ThenExpected: thenExpected{
				nodeExists:    true,
				layersContain: true,
			},
		},
		{
			Scenario: "Add a vector with dimension mismatch",
			Given:    "An index with existing vectors",
			When:     "Adding a vector with different dimension",
			Then:     "Should return error",
			GivenDetail: givenDetail{
				existingVectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
				},
			},
			WhenDetail: whenDetail{
				id:      2,
				vectors: [][]float32{{1.0, 0.0}}, // Different dimension
			},
			ThenExpected: thenExpected{
				err: "vector dimension mismatch",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Scenario, func(t *testing.T) {
			// Given
			idx0, err := NewFactory().New("test", &Config{}, Dependencies{})
			Expect(err).NotTo(HaveOccurred())
			for id, vectors := range tt.GivenDetail.existingVectors {
				err := idx0.Add(context.Background(), id, vectors)
				Expect(err).NotTo(HaveOccurred())
			}

			// When
			err = idx0.Add(context.Background(), tt.WhenDetail.id, tt.WhenDetail.vectors)

			// Then
			if tt.ThenExpected.err != "" {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(tt.ThenExpected.err))
			} else {
				Expect(err).NotTo(HaveOccurred())

				v := idx0.(*idx)
				v.mu.RLock()
				defer v.mu.RUnlock()

				if tt.ThenExpected.nodeExists {
					Expect(v.m).To(HaveKey(tt.WhenDetail.id))
					node := v.m[tt.WhenDetail.id]
					Expect(node.vectors).To(Equal(tt.WhenDetail.vectors))
				}

				if tt.ThenExpected.layersContain {
					nodeInLayers := false
					for _, id := range v.layers[0].nodes {
						if id == tt.WhenDetail.id {
							nodeInLayers = true
							break
						}
					}
					Expect(nodeInLayers).To(BeTrue(), "Node should be in layer 0")

					if len(tt.GivenDetail.existingVectors) > 0 {
						node := v.m[tt.WhenDetail.id]
						hasFriends := false
						for _, friends := range node.friendsOnLayers {
							if len(friends) > 0 {
								hasFriends = true
								break
							}
						}
						Expect(hasFriends).To(BeTrue(), "Node should have friends")
					}
				}
			}
		})
	}
}

func TestAddRejectsInvalidVectors(t *testing.T) {
	tests := []struct {
		name   string
		vector []float32
	}{
		{name: "NaN component", vector: []float32{1, float32(math.NaN())}},
		{name: "positive infinity component", vector: []float32{1, float32(math.Inf(1))}},
		{name: "negative infinity component", vector: []float32{1, float32(math.Inf(-1))}},
		{name: "zero norm", vector: []float32{0, 0}},
		{name: "underflowing norm", vector: []float32{math.SmallestNonzeroFloat32, 0}},
		{name: "non-finite norm", vector: []float32{math.MaxFloat32, math.MaxFloat32}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			vectorIndex, err := NewFactory().New("test", &Config{}, Dependencies{})
			g.Expect(err).NotTo(HaveOccurred())

			err = vectorIndex.Add(context.Background(), 42, [][]float32{{1, 0}, tt.vector})

			g.Expect(err).To(HaveOccurred())
			concrete := vectorIndex.(*idx)
			g.Expect(concrete.m).NotTo(HaveKey(uint64(42)))
		})
	}
}

func TestAddRejectsInvalidVectorForExistingID(t *testing.T) {
	g := NewWithT(t)
	vectorIndex, err := NewFactory().New("test", &Config{}, Dependencies{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(vectorIndex.Add(context.Background(), 42, [][]float32{{1, 0}})).To(Succeed())

	err = vectorIndex.Add(context.Background(), 42, [][]float32{{float32(math.NaN()), 0}})

	g.Expect(err).To(HaveOccurred())
	concrete := vectorIndex.(*idx)
	g.Expect(concrete.m[42].vectors).To(Equal([][]float32{{1, 0}}))
}

func TestSearchRejectsInvalidVectors(t *testing.T) {
	tests := []struct {
		name   string
		vector []float32
	}{
		{name: "NaN component", vector: []float32{1, float32(math.NaN())}},
		{name: "positive infinity component", vector: []float32{1, float32(math.Inf(1))}},
		{name: "negative infinity component", vector: []float32{1, float32(math.Inf(-1))}},
		{name: "zero norm", vector: []float32{0, 0}},
		{name: "underflowing norm", vector: []float32{math.SmallestNonzeroFloat32, 0}},
		{name: "non-finite norm", vector: []float32{math.MaxFloat32, math.MaxFloat32}},
	}

	for _, populated := range []bool{false, true} {
		indexState := "empty index"
		if populated {
			indexState = "populated index"
		}
		t.Run(indexState, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					g := NewWithT(t)
					vectorIndex, err := NewFactory().New("test", &Config{}, Dependencies{})
					g.Expect(err).NotTo(HaveOccurred())
					if populated {
						g.Expect(vectorIndex.Add(context.Background(), 1, [][]float32{{1, 0}})).To(Succeed())
					}

					results, err := vectorIndex.Search(context.Background(), tt.vector, 0, 10)

					g.Expect(err).To(HaveOccurred())
					g.Expect(results).To(BeNil())
				})
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	RegisterTestingT(t)

	type givenDetail struct {
		vectors map[uint64][][]float32
	}
	type whenDetail struct{}
	type thenExpected struct {
		err string
	}

	tests := []test.Case[givenDetail, whenDetail, thenExpected]{
		{
			Scenario: "Encode and decode an index with data",
			Given:    "An index with some vectors",
			When:     "Encoding and decoding the index",
			Then:     "Should restore the index correctly",
			GivenDetail: givenDetail{
				vectors: map[uint64][][]float32{
					1: {{1.0, 0.0, 0.0}},
					2: {{0.0, 1.0, 0.0}},
				},
			},
			WhenDetail:   whenDetail{},
			ThenExpected: thenExpected{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Scenario, func(t *testing.T) {
			// Given.
			original, err := NewFactory().New("test", &Config{}, Dependencies{})
			Expect(err).NotTo(HaveOccurred())
			for id, vectors := range tt.GivenDetail.vectors {
				err := original.Add(context.Background(), id, vectors)
				Expect(err).NotTo(HaveOccurred())
			}

			// When.
			var buf bytes.Buffer
			err = original.EncodeTo(context.Background(), &buf)
			Expect(err).NotTo(HaveOccurred())

			decoded, err := NewFactory().New("test", &Config{}, Dependencies{})
			Expect(err).NotTo(HaveOccurred())
			err = decoded.DecodeFrom(context.Background(), &buf)

			// Then.
			if tt.ThenExpected.err != "" {
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(tt.ThenExpected.err))
			} else {
				Expect(err).NotTo(HaveOccurred())

				// Verify by searching.
				for _, vectors := range tt.GivenDetail.vectors {
					for _, vector := range vectors {
						originalResults, err := original.Search(context.Background(), vector, 0.99, 10)
						Expect(err).NotTo(HaveOccurred())
						decodedResults, err := decoded.Search(context.Background(), vector, 0.99, 10)
						Expect(err).NotTo(HaveOccurred())

						Expect(decodedResults).To(HaveLen(len(originalResults)))
						for id, score := range originalResults {
							Expect(decodedResults).To(HaveKey(id))
							Expect(decodedResults[id]).To(BeNumerically("~", score, 0.000001))
						}
					}
				}
			}
		})
	}
}
