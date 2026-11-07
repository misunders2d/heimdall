package scheduler

import (
	"context"
	"fmt"
	"github.com/misunders2d/heimdall/internal/core"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestControlQueuedLeaseCannotUpgrade(t *testing.T) {
	r, c := controlRig(t)
	var generation atomic.Int64
	generation.Store(1)
	who, valid, err := func() (Caller, func() bool, error) {
		r.s.ResolveControl = func(int) (Caller, func() bool, error) {
			g := generation.Load()
			return asBoss, func() bool { return generation.Load() == g }, nil
		}
		return c.resolvePeer(os.Getpid(), func() bool { return true })
	}()
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	result := make(chan ControlResult, 1)
	go func() {
		result <- c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, ControlRequest{Action: "list"}, who, valid)
	}()
	generation.Store(2) // another native person input takes this same warm worker
	c.mu.Unlock()
	if got := <-result; got.Error == "" {
		t.Fatal("queued old generation upgraded", got)
	}
	// No origin at request receipt cannot borrow a future active turn.
	r.s.ResolveControl = func(int) (Caller, func() bool, error) { return asBoss, func() bool { return false }, nil }
	if _, _, err := c.resolvePeer(os.Getpid(), func() bool { return true }); err == nil {
		t.Fatal("idle request accepted")
	}
}
func TestControlLeaseExpiresDuringLiveVerification(t *testing.T) {
	r, c := controlRig(t)
	p := core.SlackPrincipal("owner@example.test", "Ttest", "")
	who := Caller{Principal: p, Role: core.RoleOwner, Conv: core.ConvKey{Interface: "slack", Chat: "Dtest", Thread: "1.1"}, Private: true}
	_, _, err := r.st.AddInbound(core.Inbound{Conv: who.Conv, From: p, Private: true, Origin: core.OriginLive, MsgID: "lease"})
	if err != nil {
		t.Fatal(err)
	}
	r.s.access.Owners[p.Key()] = true
	valid := true
	r.s.Reverify = func(context.Context, core.Principal) (Verified, bool, error) {
		valid = false
		return Verified{Principal: p}, true, nil
	}
	got := c.executePeer(context.Background(), &syscall.Ucred{Uid: 1234}, ControlRequest{Action: "list"}, who, func() bool { return valid })
	if got.Error == "" {
		t.Fatal("expired after blocking verification", got)
	}
}
func TestControlPrivateReadsStayPrivate(t *testing.T) {
	r, c := controlRig(t)
	id := r.add(asBoss, `{"name":"private name","schedule":{"every":"1h"},"prompt":"PRIVATE_CONTENT"}`)
	who := asBoss
	who.Conv.Chat = "-100"
	who.Private = false
	_, _, err := r.st.AddInbound(core.Inbound{Conv: who.Conv, From: who.Principal, Origin: core.OriginLive, MsgID: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"show", "history", "checkpoint"} {
		got := c.execute(context.Background(), 1234, ControlRequest{Action: action, ID: id}, who)
		if got.Error == "" {
			t.Fatal(action, got)
		}
	}
	for _, action := range []string{"list", "discover"} {
		got := c.execute(context.Background(), 1234, ControlRequest{Action: action}, who)
		if got.Error != "" || strings.Contains(got.Reply, "PRIVATE_CONTENT") || strings.Contains(got.Reply, "private name") {
			t.Fatal(action, got)
		}
	}
}
func TestControlPeerProcessHelper(t *testing.T) {
	socket := os.Getenv("TEST_CONTROL_PEER_SOCKET")
	if socket == "" {
		return
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		os.Exit(3)
	}
	defer conn.Close()
	fmt.Fprintln(conn, "ready")
	var b [1]byte
	_, _ = os.Stdin.Read(b[:])
	os.Exit(0)
}
func TestControlPeerLifetimeRejectsExitedProcess(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "peer.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestControlPeerProcessHelper$")
	cmd.Env = append(os.Environ(), "TEST_CONTROL_PEER_SOCKET="+socket)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	_ = l.SetDeadline(time.Now().Add(5 * time.Second))
	conn, err := l.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fd, err := peerPIDFD(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if !peerAlive(fd) {
		t.Fatal("live peer rejected")
	}
	cred, err := peerCredentials(conn)
	if err != nil || int(cred.Pid) != cmd.Process.Pid {
		t.Fatal(cred, err)
	}
	_ = input.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if peerAlive(fd) {
		t.Fatal("dead/reaped peer retained authority")
	}
	_, c := controlRig(t)
	if _, _, err := c.resolvePeer(int(cred.Pid), func() bool { return peerAlive(fd) }); err == nil {
		t.Fatal("exited peer accepted")
	}
}
