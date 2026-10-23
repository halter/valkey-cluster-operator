package controller

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	ctrl "sigs.k8s.io/controller-runtime"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	internalValkey "github.com/halter/valkey-cluster-operator/internal/controller/valkey"
)

// fakeClusterNodesServer speaks enough RESP for valkey-go's connection
// handshake and answers CLUSTER NODES with a fixed view, counting the most
// connections it has had open at once.
type fakeClusterNodesServer struct {
	listener net.Listener
	view     string
	open     atomic.Int64
	peak     atomic.Int64
	waiter   sync.WaitGroup
}

func startFakeClusterNodesServer(view string) *fakeClusterNodesServer {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	server := &fakeClusterNodesServer{listener: listener, view: view}
	server.waiter.Add(1)
	go func() {
		defer server.waiter.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			open := server.open.Add(1)
			for {
				peak := server.peak.Load()
				if open <= peak || server.peak.CompareAndSwap(peak, open) {
					break
				}
			}
			server.waiter.Add(1)
			go server.serve(conn)
		}
	}()
	return server
}

func (s *fakeClusterNodesServer) port() int {
	return s.listener.Addr().(*net.TCPAddr).Port
}

func (s *fakeClusterNodesServer) stop() {
	_ = s.listener.Close()
	s.waiter.Wait()
}

func (s *fakeClusterNodesServer) serve(conn net.Conn) {
	defer s.waiter.Done()
	defer s.open.Add(-1)
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		var reply string
		switch strings.ToUpper(strings.Join(command[:min(2, len(command))], " ")) {
		case "HELLO 3":
			reply = "%2\r\n+proto\r\n:3\r\n+version\r\n+8.0.5\r\n"
		case "CLUSTER NODES":
			reply = fmt.Sprintf("$%d\r\n%s\r\n", len(s.view), s.view)
		case "PING":
			reply = "+PONG\r\n"
		default:
			reply = "+OK\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

// readRESPCommand reads one command sent as a RESP array of bulk strings.
func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(header, "*") {
		return nil, fmt.Errorf("unexpected RESP header %q", header)
	}
	count, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil {
		return nil, err
	}
	command := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
		if err != nil {
			return nil, err
		}
		argument := make([]byte, length+2)
		if _, err := io.ReadFull(reader, argument); err != nil {
			return nil, err
		}
		command = append(command, string(argument[:length]))
	}
	return command, nil
}

var _ = Describe("meetClusterNodes", func() {
	It("holds one client per node, not one per pair of nodes", func() {
		const nodeCount = 6
		clusterNodes := make([]*internalValkey.ClusterNode, 0, nodeCount)
		viewLines := make([]string, 0, nodeCount)
		for i := 0; i < nodeCount; i++ {
			id := fmt.Sprintf("%040d", i)
			clusterNodes = append(clusterNodes, &internalValkey.ClusterNode{
				Pod: fmt.Sprintf("meet-test-%d-0", i),
				IP:  "127.0.0.1",
				ID:  id,
			})
			viewLines = append(viewLines, fmt.Sprintf("%s 127.0.0.1:6379@16379 master - 0 0 %d connected", id, i))
		}
		// Every node already knows every other, so no CLUSTER MEET is sent and
		// the step only reads each node's view.
		server := startFakeClusterNodesServer(strings.Join(viewLines, "\n") + "\n")
		defer server.stop()

		reconciler := &ValkeyClusterReconciler{}
		valkeyCluster := &cachev1alpha1.ValkeyCluster{}
		err := reconciler.meetClusterNodes(ctx, ctrl.Request{}, valkeyCluster, clusterNodes, server.port())
		Expect(err).NotTo(HaveOccurred())

		Expect(server.peak.Load()).To(BeNumerically("<=", nodeCount))
	})
})
