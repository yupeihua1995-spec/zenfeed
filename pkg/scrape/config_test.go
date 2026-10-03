// Copyright (C) 2025 wangyusong
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package scrape

import (
	"testing"

	"github.com/stretchr/testify/assert"

	appconfig "github.com/glidea/zenfeed/pkg/config"
)

func TestConfigFromSkipsDisabledSources(t *testing.T) {
	enabled := true
	disabled := false
	app := &appconfig.App{Scrape: appconfig.Scrape{Sources: []appconfig.ScrapeSource{
		{Name: "legacy-default"},
		{Name: "explicit-enabled", Enabled: &enabled},
		{Name: "disabled", Enabled: &disabled},
	}}}

	var got Config
	got.From(app)

	assert.Len(t, got.Scrapers, 2)
	assert.Equal(t, "legacy-default", got.Scrapers[0].Name)
	assert.Equal(t, "explicit-enabled", got.Scrapers[1].Name)
}
