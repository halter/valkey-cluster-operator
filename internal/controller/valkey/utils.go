package valkey

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

type ClusterNode struct {
	Pod          string
	IP           string
	Hostname     string
	ID           string
	MasterNodeID string
	Flags        []string
	SlotRanges   []*ClusterSlotRange
}

func (c *ClusterNode) String() string {
	b, _ := json.Marshal(c)
	return string(b)
}

func (c *ClusterNode) IsMaster() bool {
	for _, flag := range c.Flags {
		if flag == "master" {
			return true
		}
	}
	return false
}
func (c *ClusterNode) HasSlots() bool {
	count := SlotCount(c.SlotRanges)
	return count > 0
}
func (c *ClusterNode) SlotCount() int {
	return SlotCount(c.SlotRanges)
}

func parseClusterNodeLine(line string) (*ClusterNode, error) {
	strings.Fields(line)
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return nil, fmt.Errorf("expected len(fields) >= 4, but got %d: %v", len(fields), fields)
	}

	flagsWithoutMyself := []string{}
	flags := strings.Split(fields[2], ",")
	for _, flag := range flags {
		if flag != "myself" {
			flagsWithoutMyself = append(flagsWithoutMyself, flag)
		}
	}
	slotRanges := make([]*ClusterSlotRange, 0)
	if len(fields) > 8 {
		for i := 8; i < len(fields); i++ {
			// skip slot migration
			if strings.HasPrefix(fields[i], "[") {
				continue
			}
			if strings.Contains(fields[i], "-") {
				parts := strings.Split(fields[i], "-")
				start, err := strconv.Atoi(parts[0])
				if err != nil {
					return nil, fmt.Errorf("failed to convert string %w: line: %s", err, line)
				}
				end, err := strconv.Atoi(parts[1])
				if err != nil {
					return nil, fmt.Errorf("failed to convert string %w: line: %s", err, line)
				}
				slotRange := &ClusterSlotRange{
					Start: start,
					End:   end,
				}
				slotRanges = append(slotRanges, slotRange)
			} else {
				start, err := strconv.Atoi(fields[i])
				if err != nil {
					return nil, fmt.Errorf("failed to convert string %w: line: %s", err, line)
				}
				end := start
				slotRange := &ClusterSlotRange{
					Start: start,
					End:   end,
				}
				slotRanges = append(slotRanges, slotRange)
			}
		}
	}
	IP := strings.Split(fields[1], ":")[0]
	hostname := ""
	if strings.Contains(fields[1], ",") {
		hostname = strings.Split(fields[1], ",")[1]
	}
	ID := strings.ReplaceAll(fields[0], "txt:", "")
	MasterNodeID := fields[3]
	if MasterNodeID == "-" {
		MasterNodeID = ""
	}
	return &ClusterNode{
		IP:           IP,
		Hostname:     hostname,
		ID:           ID,
		MasterNodeID: MasterNodeID,
		Flags:        flagsWithoutMyself,
		SlotRanges:   slotRanges,
	}, nil
}

func ParseClusterNodes(clusterNodesTxt string) ([]*ClusterNode, error) {
	result := make([]*ClusterNode, 0)
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		clusterNode, err := parseClusterNodeLine(line)
		if err != nil {
			return nil, err
		}
		result = append(result, clusterNode)
	}
	return result, nil
}

func ParseClusterNode(clusterNodesTxt string) (*ClusterNode, error) {
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
			return parseClusterNodeLine(line)
		}
	}
	return nil, fmt.Errorf("Could not parse cluster nodes from text: %s", clusterNodesTxt)
}

func ParseClusterNodesExludeSelf(clusterNodesTxt string) ([]*ClusterNode, error) {
	result := make([]*ClusterNode, 0)
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
			continue
		}
		if line == "" {
			continue
		}
		clusterNode, err := parseClusterNodeLine(line)
		if err != nil {
			return nil, err
		}
		result = append(result, clusterNode)
	}
	return result, nil
}

// There are 16384 hash slots in Valkey Cluster, and to compute the hash slot for a given key, we simply take the CRC16 of the key modulo 16384.
// 0-16383

type ClusterSlotRange struct {
	Start int
	End   int
}

func (c *ClusterSlotRange) String() string {
	if c == nil {
		return "-"
	}
	return fmt.Sprintf("%d-%d", c.Start, c.End)
}

func SlotRanges(numShards int) []*ClusterSlotRange {
	hashSlots := 16384
	if numShards < 1 {
		return nil
	}

	perGroup := hashSlots / numShards

	result := make([]*ClusterSlotRange, 0)
	j := 0
	for i := 0; i < numShards; i++ {
		if i == numShards-1 {
			result = append(result, &ClusterSlotRange{Start: j, End: 16383})
			return result
		}
		result = append(result, &ClusterSlotRange{Start: j, End: j + perGroup - 1})
		j = j + perGroup
	}
	return result
}

func SlotCounts(numShards int) []int {
	ranges := SlotRanges(numShards)
	counts := make([]int, 0)
	for _, r := range ranges {
		counts = append(counts, (r.End-r.Start)+1)
	}
	return counts
}

func SlotCount(slotRanges []*ClusterSlotRange) int {
	sum := 0
	for _, slotRange := range slotRanges {
		sum = sum + (slotRange.End - slotRange.Start) + 1
	}
	return sum
}

type Reshard struct {
	FromID string
	ToID   string
	Slots  int
}

func ToStatusClusterNode(cn ClusterNode) cachev1alpha1.ValkeyClusterNode {
	return cachev1alpha1.ValkeyClusterNode{
		Pod:          cn.Pod,
		IP:           cn.IP,
		ID:           cn.ID,
		SlotRange:    fmt.Sprintf("%v", cn.SlotRanges),
		MasterNodeID: cn.MasterNodeID,
		Flags:        cn.Flags,
	}
}

func GenerateReshardingPlan(clusterNodesForShard map[int][]*ClusterNode, desiredShards int) ([]Reshard, error) {
	if len(clusterNodesForShard) == 0 {
		return nil, nil
	}

	primaries := map[int]*ClusterNode{}

	for shardIdx, clusterNodes := range clusterNodesForShard {
		for _, cn := range clusterNodes {
			if cn.IsMaster() {
				primaries[shardIdx] = cn
			}
		}
	}

	desiredSlotCounts := SlotCounts(int(desiredShards))
	actualSlotCounts := []int{}
	maxIdx := 0
	for idx := range clusterNodesForShard {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	for i := 0; i <= maxIdx; i++ {
		for _, cn := range clusterNodesForShard[i] {
			if cn.IsMaster() && cn.HasSlots() {
				actualSlotCounts = append(actualSlotCounts, cn.SlotCount())
			}
		}
	}

	// pad with 0s
	if len(desiredSlotCounts) < len(actualSlotCounts) {
		for i := 0; i < len(actualSlotCounts)-len(desiredSlotCounts); i++ {
			desiredSlotCounts = append(desiredSlotCounts, 0)
		}
	}
	if len(desiredSlotCounts) > len(actualSlotCounts) {
		for i := 0; i < len(desiredSlotCounts)-len(actualSlotCounts); i++ {
			actualSlotCounts = append(actualSlotCounts, 0)
		}
	}

	sum := 0
	for _, c := range desiredSlotCounts {
		sum = sum + c
	}
	if sum != 16384 {
		return nil, fmt.Errorf("expected there to be 16384 total desired slots but got %v", desiredSlotCounts)
	}
	sum = 0
	for _, c := range actualSlotCounts {
		sum = sum + c
	}
	if sum != 16384 {
		return nil, fmt.Errorf("expected there to be 16384 total actual slots but got %v", actualSlotCounts)
	}

	actionPlan := []Reshard{}
	rid := map[string]int{}
	receive := map[string]int{}
	for i := range actualSlotCounts {
		if actualSlotCounts[i] == desiredSlotCounts[i] {
			//all is well
		} else if actualSlotCounts[i] > desiredSlotCounts[i] {
			// need to get rid of:
			delta := actualSlotCounts[i] - desiredSlotCounts[i]
			rid[primaries[i].ID] = delta

		} else if actualSlotCounts[i] < desiredSlotCounts[i] {
			// need to get:
			delta := desiredSlotCounts[i] - actualSlotCounts[i]
			receive[primaries[i].ID] = delta
		}
	}

	for fromID := range rid {
		if rid[fromID] == 0 {
			continue
		}
		for toID, receiveSlots := range receive {
			if receiveSlots <= rid[fromID] {
				actionPlan = append(actionPlan, Reshard{
					FromID: fromID,
					ToID:   toID,
					Slots:  receiveSlots,
				})
				rid[fromID] = rid[fromID] - receiveSlots
			} else {
				actionPlan = append(actionPlan, Reshard{
					FromID: fromID,
					ToID:   toID,
					Slots:  rid[fromID],
				})
				rid[fromID] = 0
			}
		}
	}

	sort.Slice(actionPlan, func(i, j int) bool {
		if actionPlan[i].FromID == actionPlan[j].FromID {
			return actionPlan[i].ToID < actionPlan[j].ToID
		}
		return actionPlan[i].FromID < actionPlan[j].FromID
	})

	return actionPlan, nil
}

func TcpCheck(host, port string) bool {
	// check tcp port
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 2*time.Second)
	if err != nil {
		return false
	}
	if conn != nil {
		defer conn.Close()
	}
	return true
}
