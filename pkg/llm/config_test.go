package llm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFactoryConfigValidateRejectsDuplicateNames_BitsUT(t *testing.T) {
	c := &FactoryConfig{LLMs: []Config{
		{Name: "duplicate", APIKey: "first-secret", Model: "model-a"},
		{Name: "duplicate", APIKey: "second-secret", Model: "model-b"},
	}}

	require.ErrorContains(t, c.Validate(), `llm name "duplicate" must be unique`)
}
