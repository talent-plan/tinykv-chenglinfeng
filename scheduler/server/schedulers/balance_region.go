// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (a storeSlice) Len() int           { return len(a) }
func (a storeSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a storeSlice) Less(i, j int) bool { return a[i].GetRegionSize() < a[j].GetRegionSize() }

// Schedule 避免太多 region 堆积在一个 store
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. 选出所有的 suitableStores
	stores := make(storeSlice, 0)
	for _, store := range cluster.GetStores() {
		// 适合被移动的 store 需要满足停机时间不超过 MaxStoreDownTime
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	if len(stores) < 2 {
		return nil
	}
	// 2. 遍历 suitableStores，找到目标 region 和 store
	sort.Sort(stores)
	var fromStore, toStore *core.StoreInfo
	var region *core.RegionInfo
	for i := len(stores) - 1; i >= 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
	}
	if region == nil {
		return nil
	}
	// 3. 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas 直接放弃本次操作
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	// 4. 再次从 suitableStores 里面找到一个目标 store，目标 store 不能在原来的 region 里面
	for i := 0; i < len(stores); i++ {
		if _, ok := storeIds[stores[i].GetID()]; !ok {
			toStore = stores[i]
			break
		}
	}
	if toStore == nil {
		return nil
	}
	// 5. 判断两个 store 的 region size 差值是否小于 2*ApproximateSize，是的话放弃 region 移动
	if fromStore.GetRegionSize()-toStore.GetRegionSize() < region.GetApproximateSize() {
		return nil
	}
	// 6. 创建 CreateMovePeerOperator 操作并返回
	newPeer, _ := cluster.AllocPeer(toStore.GetID())
	desc := fmt.Sprintf("move-from-%d-to-%d", fromStore.GetID(), toStore.GetID())
	op, _ := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), newPeer.GetId())
	return op
}
