package cluster

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/tikv/pd/server/core"
	"go.uber.org/zap"
)

/*
mysql indexes
[tableName:=tables_priv] [tableID:=11] [indexName:=PRIMARY] [indexID:=1]
[tableName:=columns_priv] [tableID:=13] [indexName:=PRIMARY] [indexID:=1]
[tableName:=GLOBAL_VARIABLES] [tableID:=15] [indexName:=PRIMARY] [indexID:=1]
[tableName:=tidb] [tableID:=17] [indexName:=PRIMARY] [indexID:=1]
[tableName:=help_topic] [tableID:=19] [indexName:=name] [indexID:=1]
[tableName:=stats_meta] [tableID:=21] [indexName:=idx_ver] [indexID:=1]
[tableName:=stats_meta] [tableID:=21] [indexName:=tbl] [indexID:=2]
[tableName:=stats_histograms] [tableID:=23] [indexName:=tbl] [indexID:=1]
[tableName:=stats_buckets] [tableID:=25] [indexName:=tbl] [indexID:=1]
[tableName:=gc_delete_range] [tableID:=27] [indexName:=delete_range_index] [indexID:=1]
[tableName:=gc_delete_range_done] [tableID:=29] [indexName:=delete_range_done_index] [indexID:=1]
[tableName:=stats_feedback] [tableID:=31] [indexName:=hist] [indexID:=1]
[tableName:=role_edges] [tableID:=33] [indexName:=PRIMARY] [indexID:=1]
[tableName:=default_roles] [tableID:=35] [indexName:=PRIMARY] [indexID:=1]
[tableName:=bind_info] [tableID:=37] [indexName:=sql_index] [indexID:=1]
[tableName:=bind_info] [tableID:=37] [indexName:=time_index] [indexID:=2]
[tableName:=stats_top_n] [tableID:=39] [indexName:=tbl] [indexID:=1]
[tableName:=expr_pushdown_blacklist] [tableID:=41]
[tableName:=opt_rule_blacklist] [tableID:=43]
[tableName:=stats_extended] [tableID:=45] [indexName:=PRIMARY] [indexID:=1]
[tableName:=stats_extended] [tableID:=45] [indexName:=idx_1] [indexID:=2]
[tableName:=stats_extended] [tableID:=45] [indexName:=idx_2] [indexID:=3]
[tableName:=schema_index_usage] [tableID:=47] [indexName:=PRIMARY] [indexID:=1]
[tableName:=stats_fm_sketch] [tableID:=49] [indexName:=tbl] [indexID:=1]
[tableName:=user] [tableID:=5] [indexName:=PRIMARY] [indexID:=1]
[tableName:=global_grants] [tableID:=51] [indexName:=PRIMARY] [indexID:=1]
[tableName:=global_priv] [tableID:=7] [indexName:=PRIMARY] [indexID:=1]
[tableName:=db] [tableID:=9] [indexName:=PRIMARY] [indexID:=1]


[tableName:=tables_priv] [tableID:=11]
[tableName:=columns_priv] [tableID:=13]
[tableName:=GLOBAL_VARIABLES] [tableID:=15]
[tableName:=tidb] [tableID:=17]
[tableName:=help_topic] [tableID:=19]
[tableName:=stats_meta] [tableID:=21]
[tableName:=stats_histograms] [tableID:=23]
[tableName:=stats_buckets] [tableID:=25]
[tableName:=gc_delete_range] [tableID:=27]
[tableName:=gc_delete_range_done] [tableID:=29]
[tableName:=stats_feedback] [tableID:=31]
[tableName:=role_edges] [tableID:=33]
[tableName:=default_roles] [tableID:=35]
[tableName:=bind_info] [tableID:=37]
[tableName:=stats_top_n] [tableID:=39]
[tableName:=expr_pushdown_blacklist] [tableID:=41]
[tableName:=opt_rule_blacklist] [tableID:=43]
[tableName:=stats_extended] [tableID:=45]
[tableName:=schema_index_usage] [tableID:=47]
[tableName:=stats_fm_sketch] [tableID:=49]
[tableName:=user] [tableID:=5]
[tableName:=global_grants] [tableID:=51]
[tableName:=global_priv] [tableID:=7]
[tableName:=db] [tableID:=9]
*/

func testSchemaInfo(c *C, name string) *model.DBInfo {

	dbInfo := &model.DBInfo{
		Name: model.NewCIStr("mysql"),
	}

	return dbInfo
}

var _ = Suite(&mockHisHotregionsSuite{})

type mockHisHotregionsSuite struct {
}
type MockKeys struct {
	MysqlTables   []*RecordKey `json:"mysql_tables,omitempty"`
	MysqlIndexes  []*IndexKey  `json:"mysql_indexes,omitempty"`
	UnknowTables  []*RecordKey `json:"unknow_tables,omitempty"`
	UnknowIndexes []*IndexKey  `json:"unknow_indexes,omitempty"`
}

type RecordKey struct {
	TableID  int64  `json:"table_id,omitempty"`
	StartKey []byte `json:"start_key,omitempty"`
	EndKey   []byte `json:"end_key,omitempty"`
}

func (k *RecordKey) SetStartKey(b []byte) {
	k.StartKey = b
}
func (k *RecordKey) SetEndKey(b []byte) {
	k.EndKey = b
}

type IndexKey struct {
	TableID  int64  `json:"table_id,omitempty"`
	IndexID  int64  `json:"index_id,omitempty"`
	StartKey []byte `json:"start_key,omitempty"`
	EndKey   []byte `json:"end_key,omitempty"`
}

func GetMockKeysFromFile(path string) *MockKeys {
	b, _ := ioutil.ReadFile(path)
	mockKeys := &MockKeys{}
	_ = json.Unmarshal(b, mockKeys)
	return mockKeys
}

func newTestHotRegionStorageWithPathNotRRemove(pullInterval time.Duration, remianedDays int64, path string) (
	hotRegionStorage *HotRegionStorage,
	clear func(), err error) {
	ctx := context.Background()
	_, opt, err := newTestScheduleConfig()
	if err != nil {
		return nil, nil, err
	}
	raft := newTestCluster(ctx, opt).RaftCluster
	//delete data in between today and tomrrow
	hotRegionStorage, err = NewHotRegionsHistoryStorage(ctx,
		path, nil, raft, nil, remianedDays, pullInterval)
	if err != nil {
		return nil, nil, err
	}
	clear = func() {
		PrintDirSize(path)
		hotRegionStorage.Close()
		// os.RemoveAll(path)
	}
	return
}

func unixTimeMs(s string) time.Time {
	t, _ := time.ParseInLocation("2006/01/02 15:04:05", s, time.Local)
	return t
}

func keys(mockKeys *MockKeys) (tableStartKeys, tableEndKeys, indexStartKeys, indexEndKeys [][]byte) {
	for _, tbl := range mockKeys.MysqlTables {
		tableStartKeys = append(tableStartKeys, tbl.StartKey)
		tableEndKeys = append(tableEndKeys, tbl.EndKey)
	}
	for _, tbl := range mockKeys.UnknowTables {
		tableStartKeys = append(tableStartKeys, tbl.StartKey)
		tableEndKeys = append(tableEndKeys, tbl.EndKey)
	}
	for _, idx := range mockKeys.MysqlIndexes {
		indexStartKeys = append(indexStartKeys, idx.StartKey)
		indexEndKeys = append(indexEndKeys, idx.EndKey)
	}
	for _, idx := range mockKeys.UnknowIndexes {
		indexStartKeys = append(indexStartKeys, idx.StartKey)
		indexEndKeys = append(indexEndKeys, idx.EndKey)
	}
	return tableStartKeys, tableEndKeys, indexStartKeys, indexEndKeys
}

func newMockRegions(startKeys, endKeys [][]byte, n, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	// 1000, 3
	mockKeyLen := uint64(100)
	loopNum := n / mockKeyLen
	for i := uint64(0); i < loopNum; i++ {
		for j := uint64(0); j < mockKeyLen; j++ {
			num := i*loopNum + j
			peers := make([]*metapb.Peer, 0, np)
			for k := uint64(0); k < np; k++ {
				peer := &metapb.Peer{
					Id: num*np + k,
				}
				peer.StoreId = (num + k) % n
				peers = append(peers, peer)
			}
			region := &metapb.Region{
				Id:          num,
				Peers:       peers,
				StartKey:    startKeys[j],
				EndKey:      endKeys[j],
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			}
			regions = append(regions, core.NewRegionInfo(region, peers[0]))
		}
	}
	return regions
}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestHotRegionsNew(startKeys, endKeys [][]byte, n, np, startID uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: (startID+i)*np + j,
			}
			peer.StoreId = (startID + i + j) % (startID + n) // TODO should not mod in 48,52 case,use 100 to avoid mod
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:          startID + i,
			Peers:       peers,
			StartKey:    startKeys[i],
			EndKey:      endKeys[i],
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func BenchmarkInsertThreeLevelDBWithMonthHotRegions(b *testing.B) {
	mockKeys := GetMockKeysFromFile("/tmp/mock_hot_region_keys.json")
	tableStartKeys, tableEndKeys, indexStartKeys, indexEndKeys := keys(mockKeys)
	tableRegions := newTestHotRegionsNew(tableStartKeys, tableEndKeys, 48, 3, 0)
	indexRegions := newTestHotRegionsNew(indexStartKeys, indexEndKeys, 52, 3, 48)
	writeIntoLevelDB(b, tableRegions, "/tmp/hishotregions/one/pd1/hot-region", unixTimeMs("2019/03/01 00:00:00"), []string{"read", "write"}, 4320)
	writeIntoLevelDB(b, indexRegions, "/tmp/hishotregions/one/pd2/hot-region", unixTimeMs("2019/04/01 00:00:00"), []string{"read", "write"}, 4320)
	writeIntoLevelDB(b, tableRegions, "/tmp/hishotregions/one/pd3/hot-region", unixTimeMs("2019/05/01 00:00:00"), []string{"read", "write"}, 4320)
	// writeIntoLevelDB(b, tableRegions, "/tmp/hishotregions/three/pd1/hot-region", unixTimeMs("2019/03/01 00:00:00"), []string{"read", "write"}, 4320*3)
	// writeIntoLevelDB(b, indexRegions, "/tmp/hishotregions/three/pd2/hot-region", unixTimeMs("2020/06/01 00:00:00"), []string{"read", "write"}, 4320*3)
	// writeIntoLevelDB(b, tableRegions, "/tmp/hishotregions/three/pd3/hot-region", unixTimeMs("2020/09/01 00:00:00"), []string{"read", "write"}, 4320*3)
}

func writeIntoLevelDB(b *testing.B, regions []*core.RegionInfo, path string, updateTime time.Time, types []string, frequence int) {
	regionStorage, clear, err := newTestHotRegionStorageWithPathNotRRemove(10*time.Hour, -1, path)
	defer clear()
	raft := regionStorage.cluster
	for _, region := range regions {
		raft.putRegion(region)
	}
	if err != nil {
		b.Fatal(err)
	}
	regionIDs := make([]uint64, 0)
	for _, region := range raft.core.Regions.GetRegions() {
		regionIDs = append(regionIDs, region.GetMeta().Id)
	}

	sort.Slice(regionIDs, func(i, j int) bool {
		return regionIDs[i] < regionIDs[j]
	})
	log.Info("ids：", zap.Uint64s("ids:", regionIDs))
	//4320=(60*24*30)/10 * 1000 regions * 2
	for _, hotType := range types {
		writeIntoDBWithType(regionStorage, regions, frequence, updateTime, hotType)
	}
}

func writeIntoDBWithType(regionStorage *HotRegionStorage,
	regions []*core.RegionInfo, times int,
	endTime time.Time,
	hotRegionType string) time.Time {
	raft := regionStorage.cluster
	for i := 0; i < times; i++ {
		stats := newBenchmarkHotRegoinHistory(raft, endTime, regions)
		err := regionStorage.packHotRegionInfo(stats, hotRegionType)
		if err != nil {
			log.Fatal(err.Error())
		}
		regionStorage.flush()
		endTime = endTime.Add(10 * time.Minute)
	}
	return endTime
}
