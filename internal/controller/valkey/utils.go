package valkey

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
)

// ErrSlotsMigrationInFlight is returned by GenerateReshardingPlan when the
// observed per-shard slot counts do not sum to exactly 16384. While a slot is
// being migrated the importing node can claim it before the donor releases it
// (or a slot can transiently have no owner), because each node reports its own
// view of the cluster. This is not a failure of the cluster: callers should
// requeue and re-plan once the migration has settled instead of treating it as
// an error.
var ErrSlotsMigrationInFlight = errors.New("slot totals do not sum to 16384")

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
	return c.HasFlag("master")
}

func (c *ClusterNode) HasFlag(flag string) bool {
	return slices.Contains(c.Flags, flag)
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
		if strings.TrimSpace(line) == "" {
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

// FindDeadNodes returns topology entries with the hard "fail" flag (not the
// unconfirmed "fail?") whose ID is absent from liveNodeIDs.
func FindDeadNodes(topology []*ClusterNode, liveNodeIDs map[string]bool) []*ClusterNode {
	deadNodes := make([]*ClusterNode, 0)
	for _, cn := range topology {
		if liveNodeIDs[cn.ID] {
			continue
		}
		if cn.HasFlag("fail") {
			deadNodes = append(deadNodes, cn)
		}
	}
	return deadNodes
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

	desiredSlotCounts := SlotCounts(desiredShards)
	maxIdx := 0
	for idx := range clusterNodesForShard {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	// One entry per shard index so counts stay aligned with primaries,
	// including shards whose primary currently holds no slots.
	actualSlotCounts := make([]int, 0, maxIdx+1)
	for i := 0; i <= maxIdx; i++ {
		if primary, ok := primaries[i]; ok {
			actualSlotCounts = append(actualSlotCounts, primary.SlotCount())
		} else {
			actualSlotCounts = append(actualSlotCounts, 0)
		}
	}

	// pad with 0s
	for len(desiredSlotCounts) < len(actualSlotCounts) {
		desiredSlotCounts = append(desiredSlotCounts, 0)
	}
	for len(actualSlotCounts) < len(desiredSlotCounts) {
		actualSlotCounts = append(actualSlotCounts, 0)
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
		return nil, fmt.Errorf("%w: observed per-shard slot counts %v", ErrSlotsMigrationInFlight, actualSlotCounts)
	}

	actionPlan := []Reshard{}
	rid := map[string]int{}
	receive := map[string]int{}
	for i := range actualSlotCounts {
		if actualSlotCounts[i] == desiredSlotCounts[i] {
			// all is well
			continue
		}
		primary, ok := primaries[i]
		if !ok {
			return nil, fmt.Errorf("no primary node found for shard %d", i)
		}
		if actualSlotCounts[i] > desiredSlotCounts[i] {
			// need to get rid of:
			rid[primary.ID] = actualSlotCounts[i] - desiredSlotCounts[i]
		} else {
			// need to get:
			receive[primary.ID] = desiredSlotCounts[i] - actualSlotCounts[i]
		}
	}

	donorIDs := make([]string, 0, len(rid))
	for id := range rid {
		donorIDs = append(donorIDs, id)
	}
	sort.Strings(donorIDs)
	receiverIDs := make([]string, 0, len(receive))
	for id := range receive {
		receiverIDs = append(receiverIDs, id)
	}
	sort.Strings(receiverIDs)

	for _, fromID := range donorIDs {
		for _, toID := range receiverIDs {
			if rid[fromID] == 0 {
				break
			}
			if receive[toID] == 0 {
				continue
			}
			slots := min(rid[fromID], receive[toID])
			actionPlan = append(actionPlan, Reshard{
				FromID: fromID,
				ToID:   toID,
				Slots:  slots,
			})
			rid[fromID] -= slots
			receive[toID] -= slots
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

// ReplicationInfo holds parsed fields from INFO REPLICATION output.
type ReplicationInfo struct {
	Role                 string
	MasterLinkStatus     string
	MasterSyncInProgress int
	MasterReplOffset     int64
	SlaveReplOffset      int64
}

// ParseInfoReplication parses the output of INFO REPLICATION into a ReplicationInfo struct.
// The valkey-go client may prepend "txt:" to the response and uses \r\n line endings.
func ParseInfoReplication(infoTxt string) ReplicationInfo {
	info := ReplicationInfo{}
	// The valkey-go client may prepend "txt:" to the response
	infoTxt = strings.TrimPrefix(infoTxt, "txt:")
	for _, line := range strings.Split(infoTxt, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "role":
			info.Role = val
		case "master_link_status":
			info.MasterLinkStatus = val
		case "master_sync_in_progress":
			n, _ := strconv.Atoi(val)
			info.MasterSyncInProgress = n
		case "master_repl_offset":
			n, _ := strconv.ParseInt(val, 10, 64)
			info.MasterReplOffset = n
		case "slave_repl_offset":
			n, _ := strconv.ParseInt(val, 10, 64)
			info.SlaveReplOffset = n
		}
	}
	return info
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
