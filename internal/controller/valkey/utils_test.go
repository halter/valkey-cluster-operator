package valkey

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlotRanges(t *testing.T) {
	testcases := []struct {
		in  int
		out []ClusterSlotRange
	}{
		{1, []ClusterSlotRange{{0, 16383}}},
		{2, []ClusterSlotRange{{0, 8191}, {8192, 16383}}},
		{3, []ClusterSlotRange{{0, 5460}, {5461, 10921}, {10922, 16383}}},
		{4, []ClusterSlotRange{{0, 4095}, {4096, 8191}, {8192, 12287}, {12288, 16383}}},
	}

	for _, tt := range testcases {
		t.Run(fmt.Sprintf("%d", tt.in), func(t *testing.T) {
			ranges := SlotRanges(tt.in)

			if len(ranges) != len(tt.out) {
				t.Errorf("expected len(ranges) == len(tt.out), but got %d != %d", len(ranges), len(tt.out))
			}
			for i, actual := range ranges {
				if actual.Start != tt.out[i].Start {
					t.Errorf("got %d, want %d", actual.Start, tt.out[i].Start)
				}
				if actual.End != tt.out[i].End {
					t.Errorf("got %d, want %d", actual.End, tt.out[i].End)
				}
			}
		})
	}

}

func TestParsing(t *testing.T) {
	testcases := []struct {
		in  string
		out []*ClusterNode
	}{
		{`2ce359297f259ff422218053d9c38e8eee5ac3f6 10.9.15.190:6379@16379 slave 530e79a7306c62ce8edd1d1fd23ceb42f0b76529 0 1747631314000 1 connected
530e79a7306c62ce8edd1d1fd23ceb42f0b76529 10.9.0.118:6379@16379 master - 0 1747631314884 1 connected 8293-16383
552b84fa4644cdb3dd963462378dc3805568c2ae 10.9.22.221:6379@16379 slave fd5a39e1e47b38d0cfabc11388fd7230c2c0f183 0 1747631315388 0 connected
fd5a39e1e47b38d0cfabc11388fd7230c2c0f183 10.9.8.19:6379@16379 myself,master - 0 0 0 connected 101-8191
2fdd77393718a4162f1f11021139e7580914c3a6 10.9.4.229:6379@16379 slave 530e79a7306c62ce8edd1d1fd23ceb42f0b76529 0 1747631315086 1 connected
5fc914131d5e1b2cce9023dd204cc502de166d61 10.9.15.208:6379@16379 slave c46b0932f83ee1fcf139397688421f3f2845af61 0 1747631315588 7 connected
c46b0932f83ee1fcf139397688421f3f2845af61 10.9.22.107:6379@16379 master - 0 1747631314000 7 connected 0-100 8192-8292
0d8fc1b8d1fa42654d977a1851d92835ab4250e3 10.9.23.215:6379@16379 slave fd5a39e1e47b38d0cfabc11388fd7230c2c0f183 0 1747631314382 0 connected
64c28a06b360d40fc811eee290fd874fe08a140e 10.9.22.17:6379@16379 slave c46b0932f83ee1fcf139397688421f3f2845af61 0 1747631315000 7 connected`, []*ClusterNode{
			{
				IP:           "10.9.15.190",
				ID:           "2ce359297f259ff422218053d9c38e8eee5ac3f6",
				MasterNodeID: "530e79a7306c62ce8edd1d1fd23ceb42f0b76529",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
			{
				IP:           "10.9.0.118",
				ID:           "530e79a7306c62ce8edd1d1fd23ceb42f0b76529",
				MasterNodeID: "",
				Flags:        []string{"master"},
				SlotRanges:   []*ClusterSlotRange{{Start: 8293, End: 16383}},
			},
			{
				IP:           "10.9.22.221",
				ID:           "552b84fa4644cdb3dd963462378dc3805568c2ae",
				MasterNodeID: "fd5a39e1e47b38d0cfabc11388fd7230c2c0f183",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
			{
				IP:           "10.9.8.19",
				ID:           "fd5a39e1e47b38d0cfabc11388fd7230c2c0f183",
				MasterNodeID: "",
				Flags:        []string{"master"},
				SlotRanges:   []*ClusterSlotRange{&ClusterSlotRange{Start: 101, End: 8191}},
			},
			{
				IP:           "10.9.4.229",
				ID:           "2fdd77393718a4162f1f11021139e7580914c3a6",
				MasterNodeID: "530e79a7306c62ce8edd1d1fd23ceb42f0b76529",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
			{
				IP:           "10.9.15.208",
				ID:           "5fc914131d5e1b2cce9023dd204cc502de166d61",
				MasterNodeID: "c46b0932f83ee1fcf139397688421f3f2845af61",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
			{
				IP:           "10.9.22.107",
				ID:           "c46b0932f83ee1fcf139397688421f3f2845af61",
				MasterNodeID: "",
				Flags:        []string{"master"},
				SlotRanges:   []*ClusterSlotRange{&ClusterSlotRange{Start: 0, End: 100}, &ClusterSlotRange{Start: 8192, End: 8292}},
			},
			{
				IP:           "10.9.23.215",
				ID:           "0d8fc1b8d1fa42654d977a1851d92835ab4250e3",
				MasterNodeID: "fd5a39e1e47b38d0cfabc11388fd7230c2c0f183",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
			{
				IP:           "10.9.22.17",
				ID:           "64c28a06b360d40fc811eee290fd874fe08a140e",
				MasterNodeID: "c46b0932f83ee1fcf139397688421f3f2845af61",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
		}},
		{`530e79a7306c62ce8edd1d1fd23ceb42f0b76529 10.9.0.118:6379@16379 master - 0 1747631314884 1 connected 8293-16383 1 3
64c28a06b360d40fc811eee290fd874fe08a140e 10.9.22.17:6379@16379 slave c46b0932f83ee1fcf139397688421f3f2845af61 0 1747631315000 7 connected`, []*ClusterNode{
			{
				IP:           "10.9.0.118",
				ID:           "530e79a7306c62ce8edd1d1fd23ceb42f0b76529",
				MasterNodeID: "",
				Flags:        []string{"master"},
				SlotRanges:   []*ClusterSlotRange{{Start: 8293, End: 16383}, {Start: 1, End: 1}, {Start: 3, End: 3}},
			},
			{
				IP:           "10.9.22.17",
				ID:           "64c28a06b360d40fc811eee290fd874fe08a140e",
				MasterNodeID: "c46b0932f83ee1fcf139397688421f3f2845af61",
				Flags:        []string{"slave"},
				SlotRanges:   []*ClusterSlotRange{},
			},
		}},
	}
	for _, tt := range testcases {
		actual, err := ParseClusterNodes(tt.in)
		if err != nil {
			t.Fatalf("err not expected: %v", err)
		}
		for i, e := range tt.out {
			if actual[i].IP != e.IP {
				t.Errorf("%d: expected actual[i].IP == e.IP  but got %s != %s", i, actual[i].IP, e.IP)
			}
			if actual[i].ID != e.ID {
				t.Errorf("%d: expected actual[i].ID == e.ID  but got %s != %s", i, actual[i].ID, e.ID)
			}
			if actual[i].MasterNodeID != e.MasterNodeID {
				t.Errorf("%d: expected actual[i].MasterNodeID == e.MasterNodeID  but got %s != %s", i, actual[i].MasterNodeID, e.MasterNodeID)
			}
			if actual[i].IP != e.IP {
				t.Errorf("%d: expected actual[i].IP == e.IP  but got %s != %s", i, actual[i].IP, e.IP)
			}
			if len(e.Flags) != len(actual[i].Flags) {
				t.Errorf("%d: expected len(e.Flags) == len(actual[i].Flags) but got %d != %d", i, len(e.Flags), len(actual[i].Flags))
			}
			for j, flag := range e.Flags {
				if flag != actual[i].Flags[j] {
					t.Errorf("%d: expected %s but got %s", i, flag, actual[i].Flags[j])
				}
			}
			if e.SlotRanges != nil && actual[i].SlotRanges == nil {
				t.Errorf("%d: expected e.SlotRanges != nil and actual[i].SlotRanges != nil  but got  actual[i].SlotRanges == %v", i, actual[i].SlotRanges)
			}
			if e.SlotRanges == nil && actual[i].SlotRanges != nil {
				t.Errorf("%d: expected e.SlotRanges == nil and actual[i].SlotRanges == nil  but got actual[i].SlotRanges == %v", i, actual[i].SlotRanges)
			}
			if actual[i].SlotRanges != nil && e.SlotRanges != nil {
				if len(e.SlotRanges) != len(actual[i].SlotRanges) {
					t.Fatalf("%d: expected len(e.SlotRanges) != len(actual[i].SlotRanges) but got %d != %d", i, len(e.SlotRanges), len(actual[i].SlotRanges))
				}
				assert.Equal(t, e.SlotRanges, actual[i].SlotRanges)
			}
		}
	}
}

func TestGenerateReshardingPlan(t *testing.T) {
	testcases := []struct {
		clusterNodesForShard map[int][]*ClusterNode
		desiredShards        int
		plan                 []Reshard
	}{
		{
			clusterNodesForShard: map[int][]*ClusterNode{
				0: []*ClusterNode{
					{
						Pod:          "keyval-0-0",
						IP:           "10.0.0.1",
						ID:           "00000000000000000000",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{{0, 5461}},
					},
					{
						Pod:          "keyval-0-1",
						IP:           "10.0.0.2",
						ID:           "11111111111111111111",
						MasterNodeID: "00000000000000000000",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-0-2",
						IP:           "10.0.0.3",
						ID:           "33333333333333333333",
						MasterNodeID: "00000000000000000000",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
				},
				1: []*ClusterNode{
					{
						Pod:          "keyval-1-0",
						IP:           "10.0.1.1",
						ID:           "44444444444444444444",
						MasterNodeID: "55555555555555555555",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-1-1",
						IP:           "10.0.1.2",
						ID:           "55555555555555555555",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{{5462, 10923}},
					},
					{
						Pod:          "keyval-1-2",
						IP:           "10.0.1.3",
						ID:           "66666666666666666666",
						MasterNodeID: "55555555555555555555",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
				},
				2: []*ClusterNode{
					{
						Pod:          "keyval-2-0",
						IP:           "10.0.3.1",
						ID:           "77777777777777777777",
						MasterNodeID: "99999999999999999999",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
					{
						Pod:          "keyval-2-1",
						IP:           "10.0.3.2",
						ID:           "88888888888888888888",
						MasterNodeID: "99999999999999999999",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-2-2",
						IP:           "10.0.3.3",
						ID:           "99999999999999999999",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{{10924, 16383}},
					},
				},
			},
			desiredShards: 2,
			plan: []Reshard{
				{
					FromID: "99999999999999999999",
					ToID:   "00000000000000000000",
					Slots:  2730,
				},
				{
					FromID: "99999999999999999999",
					ToID:   "55555555555555555555",
					Slots:  2730,
				},
			},
		},
		{
			clusterNodesForShard: map[int][]*ClusterNode{
				0: []*ClusterNode{
					{
						Pod:          "keyval-0-0",
						IP:           "10.0.0.1",
						ID:           "00000000000000000000",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{{0, 8191}},
					},
					{
						Pod:          "keyval-0-1",
						IP:           "10.0.0.2",
						ID:           "11111111111111111111",
						MasterNodeID: "00000000000000000000",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-0-2",
						IP:           "10.0.0.3",
						ID:           "33333333333333333333",
						MasterNodeID: "00000000000000000000",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
				},
				1: []*ClusterNode{
					{
						Pod:          "keyval-1-0",
						IP:           "10.0.1.1",
						ID:           "44444444444444444444",
						MasterNodeID: "55555555555555555555",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-1-1",
						IP:           "10.0.1.2",
						ID:           "55555555555555555555",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{{8192, 16383}},
					},
					{
						Pod:          "keyval-1-2",
						IP:           "10.0.1.3",
						ID:           "66666666666666666666",
						MasterNodeID: "55555555555555555555",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
				},
				2: []*ClusterNode{
					{
						Pod:          "keyval-2-0",
						IP:           "10.0.3.1",
						ID:           "77777777777777777777",
						MasterNodeID: "99999999999999999999",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{{}},
					},
					{
						Pod:          "keyval-2-1",
						IP:           "10.0.3.2",
						ID:           "88888888888888888888",
						MasterNodeID: "99999999999999999999",
						Flags:        []string{"slave"},
						SlotRanges:   []*ClusterSlotRange{},
					},
					{
						Pod:          "keyval-2-2",
						IP:           "10.0.3.3",
						ID:           "99999999999999999999",
						MasterNodeID: "",
						Flags:        []string{"master"},
						SlotRanges:   []*ClusterSlotRange{},
					},
				},
			},
			desiredShards: 3,
			plan: []Reshard{
				{
					FromID: "00000000000000000000",
					ToID:   "99999999999999999999",
					Slots:  2731,
				},
				{
					FromID: "55555555555555555555",
					ToID:   "99999999999999999999",
					Slots:  2731,
				},
			},
		},
	}
	for _, tt := range testcases {
		actual, err := GenerateReshardingPlan(tt.clusterNodesForShard, tt.desiredShards)
		assert.NoError(t, err)
		assert.Equal(t, tt.plan, actual)
	}
}
