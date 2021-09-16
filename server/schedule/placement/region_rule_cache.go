// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/pd/pkg/slice"
	"github.com/tikv/pd/server/core"
)

// RegionRuleFitCacheManager stores each region's RegionFit Result and involving variables
// only when the RegionFit result is satisfied with its rules
// RegionRuleFitCacheManager caches RegionFit result for each region only when:
// 1. region have no down peers
// 2. RegionFit is satisfied
// RegionRuleFitCacheManager will invalid the cache for the region only when:
// 1. region peer topology is changed
// 2. region have down peers
// 3. region leader is changed
// 4. any involved rule is changed
// 5. stores topology is changed
// 6. any store label is changed
// 7. any store state is changed
type RegionRuleFitCacheManager struct {
	mu     sync.RWMutex
	caches map[uint64]*RegionRuleFitCache
}

// NewRegionRuleFitCacheManager returns RegionRuleFitCacheManager
func NewRegionRuleFitCacheManager() *RegionRuleFitCacheManager {
	return &RegionRuleFitCacheManager{
		caches: map[uint64]*RegionRuleFitCache{},
	}
}

// Invalid invalid cache by regionID
func (manager *RegionRuleFitCacheManager) Invalid(regionID uint64) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	delete(manager.caches, regionID)
}

// CheckAndGetCache checks whether the region and rules are changed for the stored cache
// If the check pass, it will return the cache
func (manager *RegionRuleFitCacheManager) CheckAndGetCache(region *core.RegionInfo,
	rules []*Rule,
	stores []*core.StoreInfo) (bool, *RegionFit) {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if cache, ok := manager.caches[region.GetID()]; ok && cache.bestFit != nil {
		if cache.IsUnchanged(region, rules, stores) {
			return true, cache.bestFit
		}
	}
	return false, nil
}

// SetCache stores RegionFit cache
func (manager *RegionRuleFitCacheManager) SetCache(region *core.RegionInfo, fit *RegionFit) {
	if !validateRegion(region) || !validateFit(fit) {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	fit.SetCached(true)
	manager.caches[region.GetID()] = toRegionRuleFitCache(region, fit)
}

// RegionRuleFitCache stores regions RegionFit result and involving variables
type RegionRuleFitCache struct {
	region       regionCache
	regionStores []storeCache
	rules        []ruleCache
	bestFit      *RegionFit
}

// IsUnchanged checks whether the region and rules unchanged for the cache
func (cache *RegionRuleFitCache) IsUnchanged(region *core.RegionInfo, rules []*Rule, stores []*core.StoreInfo) bool {
	return cache.isRegionUnchanged(region) && rulesEqual(cache.rules, rules) && storesEqual(cache.regionStores, stores)
}

func (cache *RegionRuleFitCache) isRegionUnchanged(region *core.RegionInfo) bool {
	// we only cache region when it is valid
	if !validateRegion(region) {
		return false
	}
	return region.GetLeader().StoreId == cache.region.leaderStoreID &&
		cache.region.epochEqual(region)
}

func rulesEqual(ruleCaches []ruleCache, rules []*Rule) bool {
	if len(ruleCaches) != len(rules) {
		return false
	}
	return slice.AllOf(ruleCaches, func(i int) bool {
		return slice.AnyOf(rules, func(j int) bool {
			return ruleCaches[i].ruleEqual(rules[j])
		})
	})
}

func storesEqual(a []storeCache, b []*core.StoreInfo) bool {
	if len(a) != len(b) {
		return false
	}
	return slice.AllOf(a, func(i int) bool {
		return slice.AnyOf(b, func(j int) bool {
			return a[i].storeEqual(b[j])
		})
	})
}

func validateRegion(region *core.RegionInfo) bool {
	return region != nil && region.GetLeader() != nil && len(region.GetDownPeers()) == 0 && region.GetRegionEpoch() != nil
}

func validateFit(fit *RegionFit) bool {
	return fit != nil && fit.rules != nil && len(fit.rules) > 0 && fit.regionStores != nil && len(fit.regionStores) > 0
}

func toRegionRuleFitCache(region *core.RegionInfo, fit *RegionFit) *RegionRuleFitCache {
	return &RegionRuleFitCache{
		region:       toRegionCache(region),
		regionStores: toStoreCacheList(fit.regionStores),
		rules:        toRuleCacheList(fit.rules),
		bestFit:      fit,
	}
}

type ruleCache struct {
	id       string
	group    string
	version  uint64
	createTS uint64
}

func (r ruleCache) ruleEqual(rule *Rule) bool {
	if rule == nil {
		return false
	}
	return r.id == rule.ID && r.group == rule.GroupID && r.version == rule.Version && r.createTS == rule.CreateTimestamp
}

func toRuleCacheList(rules []*Rule) (c []ruleCache) {
	for _, rule := range rules {
		c = append(c, ruleCache{
			id:       rule.ID,
			group:    rule.GroupID,
			version:  rule.Version,
			createTS: rule.CreateTimestamp,
		})
	}
	return c
}

type storeCache struct {
	storeID uint64
	labels  map[string]string
	state   metapb.StoreState
}

func (s storeCache) storeEqual(store *core.StoreInfo) bool {
	if store == nil {
		return false
	}
	return s.storeID == store.GetID() &&
		s.state == store.GetState() &&
		labelEqual(s.labels, store.GetLabels())
}

func toStoreCacheList(stores []*core.StoreInfo) (c []storeCache) {
	for _, s := range stores {
		m := make(map[string]string)
		for _, label := range s.GetLabels() {
			m[label.GetKey()] = label.GetValue()
		}
		c = append(c, storeCache{
			storeID: s.GetID(),
			labels:  m,
			state:   s.GetState(),
		})
	}
	return c
}

func labelEqual(label1 map[string]string, label2 []*metapb.StoreLabel) bool {
	if len(label1) != len(label2) {
		return false
	}
	return slice.AllOf(label2, func(i int) bool {
		k, v := label2[i].Key, label2[i].Value
		v1, ok := label1[k]
		return ok && v == v1
	})
}

type regionCache struct {
	regionID      uint64
	leaderStoreID uint64
	confVer       uint64
	version       uint64
}

func (r regionCache) epochEqual(region *core.RegionInfo) bool {
	v := region.GetRegionEpoch()
	if v == nil {
		return false
	}
	return r.confVer == v.ConfVer && r.version == v.Version
}

func toRegionCache(r *core.RegionInfo) regionCache {
	return regionCache{
		regionID:      r.GetID(),
		leaderStoreID: r.GetLeader().StoreId,
		confVer:       r.GetRegionEpoch().ConfVer,
		version:       r.GetRegionEpoch().Version,
	}
}
