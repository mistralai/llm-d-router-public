/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package prefixcacheaffinity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrlatency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// makeEndpoint creates a test endpoint with the given prefix cache match ratio
// (prefixMatch out of 100 total blocks), predicted TTFT, and in-flight tokens.
func makeEndpoint(name string, prefixMatch int, ttft float64, tokens int64) fwksched.Endpoint {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
	}
	ep := fwksched.NewEndpoint(meta, &fwkdl.Metrics{}, fwkdl.NewAttributes())
	if prefixMatch >= 0 {
		ep.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(prefixMatch, 100, 16))
	}
	if ttft >= 0 {
		ep.Put(attrlatency.LatencyPredictionInfoDataKey.String(), attrlatency.NewLatencyPredictionInfo(true, true, 0, 0, ttft, 0, 0))
	}
	if tokens >= 0 {
		ep.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Tokens: tokens})
	}
	return ep
}

func newTestPlugin(config Config) *Plugin {
	return &Plugin{
		typedName:                    fwkplugin.TypedName{Type: PluginType, Name: "test"},
		config:                       config,
		prefixMatchDataKey:           attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(config.PrefixMatchInfoProducerName),
		latencyPredictionInfoDataKey: attrlatency.LatencyPredictionInfoDataKey.WithNonEmptyProducerName(config.LatencyPredictionInfoProducerName),
		inFlightLoadDataKey:          attrconcurrency.InFlightLoadDataKey.WithNonEmptyProducerName(config.InFlightLoadProducerName),
	}
}

func TestFilter_AffinityThresholdDisabled(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 0, 10, 0),
		makeEndpoint("b", 90, 20, 0),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 2, len(result), "affinityThreshold=0 should return all")
}

func TestFilter_SingleEndpoint(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80})
	endpoints := []fwksched.Endpoint{makeEndpoint("a", 90, 10, 0)}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 1, len(result), "single endpoint should always pass")
}

func TestFilter_NoStickyEndpoints(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 10, 10, 0),
		makeEndpoint("b", 20, 20, 0),
		makeEndpoint("c", 50, 30, 0),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 3, len(result), "no sticky endpoints should return all")
}

func TestFilter_NarrowToSticky(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0, MaxTTFTPenaltyMs: 5000, TTFTSource: TTFTSourceLatencyPredictor})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 100, 0),
		makeEndpoint("b", 85, 120, 0),
		makeEndpoint("c", 10, 50, 0),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 2, len(result), "should narrow to sticky endpoints")
}

func TestFilter_TTFTPenaltyBreaksStickiness(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0, MaxTTFTPenaltyMs: 100, TTFTSource: TTFTSourceLatencyPredictor})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 500, 0),
		makeEndpoint("b", 10, 50, 0),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 2, len(result), "TTFT penalty should break stickiness")
}

// With PeakPrefillThroughput=1000 tokens/sec, in-flight tokens map to TTFT as
// tokens/1000*1000 = tokens ms: endpoint "a" -> 500ms, "b" -> 50ms.
func TestFilter_ThroughputTTFTBreaksStickiness(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0, MaxTTFTPenaltyMs: 100, TTFTSource: TTFTSourcePrefillThroughput, PeakPrefillThroughput: 1000})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 10, 500),
		makeEndpoint("b", 10, 10, 50),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 2, len(result), "throughput-derived TTFT penalty should break stickiness")
}

func TestFilter_ThroughputTTFTWithinThreshold(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0, MaxTTFTPenaltyMs: 1000, TTFTSource: TTFTSourcePrefillThroughput, PeakPrefillThroughput: 1000})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 10, 500),
		makeEndpoint("b", 10, 10, 50),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 1, len(result), "throughput-derived TTFT within threshold should NOT break stickiness")
	assert.Equal(t, "a", result[0].GetMetadata().NamespacedName.Name)
}

func TestFilter_TTFTPenaltyDisabled(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 0, MaxTTFTPenaltyMs: 0, TTFTSource: TTFTSourcePrefillThroughput, PeakPrefillThroughput: 1000})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 10, 5000), // Huge load
		makeEndpoint("b", 10, 10, 50),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 1, len(result), "maxTTFTPenaltyMs=0 should NOT break stickiness")
	assert.Equal(t, "a", result[0].GetMetadata().NamespacedName.Name)
}

func TestFilter_ExplorationProbability(t *testing.T) {
	p := newTestPlugin(Config{AffinityThreshold: 0.80, ExplorationProbability: 1.0})
	endpoints := []fwksched.Endpoint{
		makeEndpoint("a", 90, 100, 0),
		makeEndpoint("b", 10, 50, 0),
	}
	result := p.Filter(context.Background(), nil, endpoints)
	assert.Equal(t, 2, len(result), "epsilon=1.0 should always skip gate")
}

func TestConsumes_ConditionalAttributes(t *testing.T) {
	// Gate disabled: neither TTFT source is consumed.
	p := newTestPlugin(Config{MaxTTFTPenaltyMs: 0})
	consumed := p.Consumes()
	_, ok := consumed.Required[p.inFlightLoadDataKey]
	assert.False(t, ok, "InFlightLoadDataKey should not be consumed when the gate is disabled")
	_, ok = consumed.Required[p.latencyPredictionInfoDataKey]
	assert.False(t, ok, "LatencyPredictionInfoDataKey should not be consumed when the gate is disabled")

	// Gate using the latency predictor.
	p = newTestPlugin(Config{MaxTTFTPenaltyMs: 5000, TTFTSource: TTFTSourceLatencyPredictor})
	consumed = p.Consumes()
	_, ok = consumed.Required[p.latencyPredictionInfoDataKey]
	assert.True(t, ok)
	_, ok = consumed.Required[p.inFlightLoadDataKey]
	assert.False(t, ok)

	// Gate using peak prefill throughput.
	p = newTestPlugin(Config{MaxTTFTPenaltyMs: 5000, TTFTSource: TTFTSourcePrefillThroughput, PeakPrefillThroughput: 1000})
	consumed = p.Consumes()
	_, ok = consumed.Required[p.inFlightLoadDataKey]
	assert.True(t, ok)
	_, ok = consumed.Required[p.latencyPredictionInfoDataKey]
	assert.False(t, ok)
}

func TestFactory_ValidConfig(t *testing.T) {
	plugin, err := Factory("test", fwkplugin.StrictDecoder(nil), nil)
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.Equal(t, PluginType, plugin.TypedName().Type)
}

func TestFactory_PartialConfigPreservesDefaults(t *testing.T) {
	// Setting only affinityThreshold should preserve defaults for other params.
	plugin, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"affinityThreshold": 0.95}`)), nil)
	assert.NoError(t, err)
	p := plugin.(*Plugin)
	assert.Equal(t, 0.95, p.config.AffinityThreshold)
	assert.Equal(t, DefaultConfig.ExplorationProbability, p.config.ExplorationProbability)
	assert.Equal(t, DefaultConfig.MaxTTFTPenaltyMs, p.config.MaxTTFTPenaltyMs)

	// Setting only explorationProbability should preserve defaults for other params.
	plugin, err = Factory("test", fwkplugin.StrictDecoder([]byte(`{"explorationProbability": 0.05}`)), nil)
	assert.NoError(t, err)
	p = plugin.(*Plugin)
	assert.Equal(t, DefaultConfig.AffinityThreshold, p.config.AffinityThreshold)
	assert.Equal(t, 0.05, p.config.ExplorationProbability)
	assert.Equal(t, DefaultConfig.MaxTTFTPenaltyMs, p.config.MaxTTFTPenaltyMs)

	// Setting only maxTTFTPenaltyMs should preserve defaults for other params.
	plugin, err = Factory("test", fwkplugin.StrictDecoder([]byte(`{"maxTTFTPenaltyMs": 10000}`)), nil)
	assert.NoError(t, err)
	p = plugin.(*Plugin)
	assert.Equal(t, DefaultConfig.AffinityThreshold, p.config.AffinityThreshold)
	assert.Equal(t, DefaultConfig.ExplorationProbability, p.config.ExplorationProbability)
	assert.Equal(t, float64(10000), p.config.MaxTTFTPenaltyMs)
	assert.Equal(t, DefaultConfig.TTFTSource, p.config.TTFTSource)
	assert.Equal(t, DefaultConfig.PeakPrefillThroughput, p.config.PeakPrefillThroughput)
}

func TestFactory_InvalidAffinityThreshold(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"affinityThreshold": 1.5}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "affinityThreshold must be <= 1.0")
}

func TestFactory_InvalidExplorationProbability(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"explorationProbability": -0.1}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "explorationProbability must be in [0, 1]")
}

func TestFactory_InvalidPeakPrefillThroughput(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"peakPrefillThroughput": -1}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peakPrefillThroughput must be >= 0")
}

// The throughput TTFT source needs a non-zero divisor: with the gate enabled
// (maxTTFTPenaltyMs defaults to 5000) and ttftSource=prefillThroughput,
// peakPrefillThroughput=0 must be rejected.
func TestFactory_ThroughputModeRequiresPeakPrefillThroughput(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"ttftSource": "prefillThroughput", "peakPrefillThroughput": 0}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peakPrefillThroughput must be > 0 when ttftSource is prefillThroughput")
}

func TestFactory_ThroughputModeValid(t *testing.T) {
	plugin, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"ttftSource": "prefillThroughput", "peakPrefillThroughput": 1000}`)), nil)
	assert.NoError(t, err)
	p := plugin.(*Plugin)
	assert.Equal(t, TTFTSourcePrefillThroughput, p.config.TTFTSource)
	assert.Equal(t, float64(1000), p.config.PeakPrefillThroughput)
}

// An unrecognized ttftSource value is rejected rather than silently treated as
// the default.
func TestFactory_InvalidTTFTSource(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"ttftSource": "bogus"}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ttftSource must be")
}

// An empty ttftSource is rejected rather than silently defaulted: the default is
// supplied by DefaultConfig, so an explicit empty value is a configuration error.
func TestFactory_EmptyTTFTSourceRejected(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"ttftSource": ""}`)), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ttftSource must be")
}

// peakPrefillThroughput=0 is valid as long as the throughput source is unused:
// either the gate is disabled (maxTTFTPenaltyMs=0) or the latency predictor
// supplies TTFT.
func TestFactory_ZeroPeakPrefillThroughputAllowedWhenUnused(t *testing.T) {
	_, err := Factory("test", fwkplugin.StrictDecoder([]byte(`{"maxTTFTPenaltyMs": 0, "ttftSource": "prefillThroughput", "peakPrefillThroughput": 0}`)), nil)
	assert.NoError(t, err, "throughput source unused when the gate is disabled")

	_, err = Factory("test", fwkplugin.StrictDecoder([]byte(`{"ttftSource": "latencyPredictor", "peakPrefillThroughput": 0}`)), nil)
	assert.NoError(t, err, "throughput source unused when the latency predictor supplies TTFT")
}

// The default TTFT source is prefillThroughput, so an unset ttftSource selects
// the throughput estimate: it consumes InFlightLoad and requires a non-zero
// peakPrefillThroughput when the gate is enabled.
func TestFactory_DefaultsToPrefillThroughput(t *testing.T) {
	assert.Equal(t, TTFTSourcePrefillThroughput, DefaultConfig.TTFTSource)

	plugin, err := Factory("test", fwkplugin.StrictDecoder(nil), nil)
	assert.NoError(t, err)
	p := plugin.(*Plugin)
	assert.Equal(t, TTFTSourcePrefillThroughput, p.config.TTFTSource)

	_, err = Factory("test", fwkplugin.StrictDecoder([]byte(`{"peakPrefillThroughput": 0}`)), nil)
	assert.Error(t, err, "default throughput source needs a non-zero peakPrefillThroughput")
	assert.Contains(t, err.Error(), "peakPrefillThroughput must be > 0")
}

// --- break-even (matchedTokens) penalty mode ---------------------------------
//
// makeEndpoint builds PrefixCacheMatchInfo(match, 100, 16), so an endpoint with
// match=90 holds 90*16 = 1440 tokens of the request. With
// PeakPrefillThroughput=1000, in-flight tokens map to TTFT as tokens ms.

func matchedTokensPlugin(maxPenaltyMs float64) *Plugin {
	return newTestPlugin(Config{
		AffinityThreshold:     0.80,
		MaxTTFTPenaltyMs:      maxPenaltyMs,
		TTFTSource:            TTFTSourcePrefillThroughput,
		PeakPrefillThroughput: 1000,
		PenaltyMode:           PenaltyModeMatchedTokens,
	})
}

func TestBreakEven_KeepsPinWhenGapBelowMatchedTokens(t *testing.T) {
	p := matchedTokensPlugin(18000)
	// sticky holds 1440 tokens; it is only 1000 tokens more loaded than the
	// best alternative, so the cache still pays for itself.
	eps := []fwksched.Endpoint{
		makeEndpoint("sticky", 90, -1, 2000),
		makeEndpoint("free", 10, -1, 1000),
	}
	got := p.Filter(context.Background(), nil, eps)
	assert.Len(t, got, 1, "pin should hold: gap 1000 < matched 1440")
}

func TestBreakEven_ReleasesPinWhenGapExceedsMatchedTokens(t *testing.T) {
	p := matchedTokensPlugin(18000)
	// Same cache value, but the sticky endpoint is now 4000 tokens more
	// loaded, which outweighs the 1440 tokens of prefill it would save.
	eps := []fwksched.Endpoint{
		makeEndpoint("sticky", 90, -1, 5000),
		makeEndpoint("free", 10, -1, 1000),
	}
	got := p.Filter(context.Background(), nil, eps)
	assert.Len(t, got, 2, "pin should break: gap 4000 > matched 1440")
}

func TestBreakEven_ScalesWithCacheValue(t *testing.T) {
	// The point of the mode: the bar moves with how much the request has
	// cached. Same 4000-token gap, larger prefix -> pin now worth keeping.
	p := matchedTokensPlugin(18000)
	eps := []fwksched.Endpoint{
		makeEndpoint("sticky", 100, -1, 5000), // 100*16 = 1600... still < 4000
		makeEndpoint("free", 10, -1, 1000),
	}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 2,
		"1600 matched tokens does not cover a 4000 gap")

	// A bigger block size is what makes a prefix genuinely expensive to lose.
	big := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: types.NamespacedName{Name: "sticky-big", Namespace: "default"}},
		&fwkdl.Metrics{}, fwkdl.NewAttributes())
	big.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(90, 100, 128))
	big.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Tokens: 5000})
	eps = []fwksched.Endpoint{big, makeEndpoint("free", 10, -1, 1000)}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 1,
		"90*128 = 11520 matched tokens covers a 4000 gap")
}

func TestBreakEven_CeilingStillApplies(t *testing.T) {
	// A large cached prefix would justify an unbounded queue; MaxTTFTPenaltyMs
	// caps it. gap 4000 tokens = 4000ms at PPT 1000, over the 100ms ceiling.
	p := matchedTokensPlugin(100)
	big := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: types.NamespacedName{Name: "sticky", Namespace: "default"}},
		&fwkdl.Metrics{}, fwkdl.NewAttributes())
	big.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(90, 100, 1024))
	big.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Tokens: 5000})
	eps := []fwksched.Endpoint{big, makeEndpoint("free", 10, -1, 1000)}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 2,
		"ceiling should fire even though the prefix is worth more than the gap")
}

func TestBreakEven_NoMatchInfoIsNoOp(t *testing.T) {
	// A sticky endpoint with no readable match attribute yields 0 matched
	// tokens. That must not be read as "cache worth nothing, always release".
	p := matchedTokensPlugin(18000)
	sticky := fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: types.NamespacedName{Name: "sticky", Namespace: "default"}},
		&fwkdl.Metrics{}, fwkdl.NewAttributes())
	sticky.Put(attrprefix.PrefixCacheMatchInfoDataKey.String(), attrprefix.NewPrefixCacheMatchInfo(90, 100, 0))
	sticky.Put(attrconcurrency.InFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Tokens: 9000})
	eps := []fwksched.Endpoint{sticky, makeEndpoint("free", 10, -1, 1000)}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 1,
		"missing block size should leave the pin alone, not release it")
}

func TestStaticModeUnchanged(t *testing.T) {
	// Regression: the default must behave exactly as before.
	p := newTestPlugin(Config{
		AffinityThreshold:     0.80,
		MaxTTFTPenaltyMs:      1000,
		TTFTSource:            TTFTSourcePrefillThroughput,
		PeakPrefillThroughput: 1000,
		PenaltyMode:           PenaltyModeStatic,
	})
	// gap 4000 tokens = 4000ms > 1000ms ceiling -> release.
	eps := []fwksched.Endpoint{
		makeEndpoint("sticky", 90, -1, 5000),
		makeEndpoint("free", 10, -1, 1000),
	}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 2)

	// Under the ceiling -> hold, regardless of matched tokens.
	eps = []fwksched.Endpoint{
		makeEndpoint("sticky", 90, -1, 1500),
		makeEndpoint("free", 10, -1, 1000),
	}
	assert.Len(t, p.Filter(context.Background(), nil, eps), 1)
}

func TestFactory_DefaultPenaltyModeIsStatic(t *testing.T) {
	p, err := Factory("test", nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, PenaltyModeStatic, p.(*Plugin).config.PenaltyMode)
}

func TestConfigValidate_MatchedTokensRejectsLatencyPredictor(t *testing.T) {
	c := Config{
		AffinityThreshold: 0.80,
		MaxTTFTPenaltyMs:  18000,
		TTFTSource:        TTFTSourceLatencyPredictor,
		PenaltyMode:       PenaltyModeMatchedTokens,
	}
	err := c.validate()
	assert.Error(t, err, "ms and tokens are not commensurable")
	assert.Contains(t, err.Error(), "requires ttftSource")
}

func TestConfigValidate_RejectsUnknownPenaltyMode(t *testing.T) {
	c := Config{
		AffinityThreshold:     0.80,
		TTFTSource:            TTFTSourcePrefillThroughput,
		PeakPrefillThroughput: 1000,
		PenaltyMode:           PenaltyMode("nonsense"),
	}
	assert.Error(t, c.validate())
}
