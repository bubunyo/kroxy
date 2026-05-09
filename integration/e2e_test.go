//go:build integration

// Package integration runs end-to-end tests against a real Apache Kafka
// container brought up with testcontainers-go. The kroxy proxy is started
// in-process and pointed at the container's bootstrap address.
//
// Run with:  go test -tags=integration ./integration/... -count=1 -timeout=5m
package integration_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bubunyo/kroxy/admin"
	"github.com/bubunyo/kroxy/observability"
	"github.com/bubunyo/kroxy/proxy"
	"github.com/bubunyo/kroxy/resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

const (
	tenantA = "tenantA"

	tenantB = "tenantB"
)

// startProxy boots an in-process kroxy server pointed at upstream and returns
// the address clients should dial plus a stop func.
func startProxy(t *testing.T, upstream string) (addr string, stop func()) {
	t.Helper()

	res, err := resolver.NewMemory([]resolver.MemoryUser{
		{
			Username: tenantA, Password: tenantA,
			TenantID: "tenantA", TopicPrefix: tenantA + ".", Upstream: upstream,
		},
		{
			Username: tenantB, Password: tenantB,
			TenantID: "tenantB", TopicPrefix: tenantB + ".", Upstream: upstream,
		},
	})
	require.NoError(t, err)

	// Reserve a port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr = ln.Addr().String()
	require.NoError(t, ln.Close())

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := proxy.NewServer(proxy.ServerConfig{Listen: addr, Advertised: addr}, res, observability.NewMetrics(), log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Run(ctx)
		close(done)
	}()

	// Wait for listener to be up.
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, dErr := net.Dial("tcp", addr)
		if dErr == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("proxy never came up: %v", dErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return addr, func() {
		cancel()
		srv.Wait()
		<-done
	}
}

func newClient(t *testing.T, addr, user, pw string, extra ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.SASL(plain.Auth{User: user, Pass: pw}.AsMechanism()),
		kgo.RequestTimeoutOverhead(15 * time.Second),
		kgo.MetadataMinAge(100 * time.Millisecond),
	}
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	return cl
}

// TestEndToEnd_ProduceConsumeThroughProxy brings up a real Kafka, starts
// kroxy in-process, and verifies that a tenant can produce and consume
// records via the proxy. It also verifies the stored topic name on the
// broker carries the tenant prefix while the client only ever sees the
// unprefixed name.
func TestEndToEnd_ProduceConsumeThroughProxy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.7.0",
		tckafka.WithClusterID("integration-test-cluster"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)
	upstream := brokers[0]

	proxyAddr, stop := startProxy(t, upstream)
	defer stop()

	// Producer.
	prod := newClient(t, proxyAddr, tenantA, tenantA,
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic("orders"),
	)
	defer prod.Close()

	const recordCount = 10
	pCtx, pCancel := context.WithTimeout(ctx, 60*time.Second)
	defer pCancel()
	for i := 0; i < recordCount; i++ {
		r := &kgo.Record{Value: []byte(fmt.Sprintf("order-%d", i))}
		results := prod.ProduceSync(pCtx, r)
		require.NoError(t, results.FirstErr(), "produce failed at %d", i)
	}

	// Consumer in a fresh group.
	cons := newClient(t, proxyAddr, tenantA, tenantA,
		kgo.ConsumerGroup("orders-readers"),
		kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cons.Close()

	got := make([]string, 0, recordCount)
	cCtx, cCancel := context.WithTimeout(ctx, 60*time.Second)
	defer cCancel()
	for len(got) < recordCount {
		fetches := cons.PollFetches(cCtx)
		require.False(t, fetches.IsClientClosed())
		fetches.EachError(func(_ string, _ int32, err error) {
			t.Fatalf("consumer error: %v", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			got = append(got, string(r.Value))
			// Critical: the client must see the *unprefixed* topic name.
			assert.Equal(t, "orders", r.Topic)
		})
	}

	want := make([]string, recordCount)
	for i := range want {
		want[i] = fmt.Sprintf("order-%d", i)
	}
	assert.ElementsMatch(t, want, got)

	// Now talk to the broker directly (PLAINTEXT, no proxy) and confirm the
	// stored topic name carries the tenant prefix.
	direct, err := kgo.NewClient(
		kgo.SeedBrokers(upstream),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	require.NoError(t, err)
	defer direct.Close()

	mdReq := kmsg.NewPtrMetadataRequest()
	mdResp, err := mdReq.RequestWith(ctx, direct)
	require.NoError(t, err)

	var topicNames []string
	for _, t := range mdResp.Topics {
		if t.Topic != nil {
			topicNames = append(topicNames, *t.Topic)
		}
	}
	assert.Contains(t, topicNames, tenantA+".orders")
}

// TestEndToEnd_TenantIsolation verifies that one tenant cannot see another
// tenant's topics in metadata, even when both share the upstream cluster.
func TestEndToEnd_TenantIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.7.0",
		tckafka.WithClusterID("integration-isolation-cluster"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	upstream := brokers[0]

	proxyAddr, stop := startProxy(t, upstream)
	defer stop()

	// Tenant A creates a topic implicitly via produce.
	prodA := newClient(t, proxyAddr, tenantA, tenantA,
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic("private-events"),
	)
	defer prodA.Close()
	require.NoError(t, prodA.ProduceSync(ctx,
		&kgo.Record{Value: []byte("hello-from-A")}).FirstErr())

	// Tenant B asks for metadata of "private-events" — must not see A's data.
	clB := newClient(t, proxyAddr, tenantB, tenantB)
	defer clB.Close()

	mdReq := kmsg.NewPtrMetadataRequest()
	mdResp, err := mdReq.RequestWith(ctx, clB)
	require.NoError(t, err)

	for _, top := range mdResp.Topics {
		if top.Topic == nil {
			continue
		}
		assert.NotContains(t, *top.Topic, tenantA+".",
			"tenant B leaked tenant A's topic: %s", *top.Topic)
	}
}

// TestEndToEnd_AdminSetThenAuth boots Kafka + kroxy with an empty resolver,
// uses the admin RPC to register a brand-new tenant, then SASL-authenticates
// a client with those credentials and produces a record.
func TestEndToEnd_AdminSetThenAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.7.0",
		tckafka.WithClusterID("integration-admin-cluster"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	upstream := brokers[0]

	store, err := resolver.NewMemory(nil)
	require.NoError(t, err)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Reserve proxy + admin ports.
	pl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxyAddr := pl.Addr().String()
	require.NoError(t, pl.Close())

	al, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminAddr := al.Addr().String()
	require.NoError(t, al.Close())

	srv := proxy.NewServer(proxy.ServerConfig{Listen: proxyAddr, Advertised: proxyAddr}, store, observability.NewMetrics(), log)
	pCtx, pCancel := context.WithCancel(ctx)
	defer pCancel()

	proxyDone := make(chan struct{})
	go func() {
		_ = srv.Run(pCtx)
		close(proxyDone)
	}()
	t.Cleanup(func() {
		pCancel()
		srv.Wait()
		<-proxyDone
	})

	adminDone := make(chan struct{})
	go func() {
		_ = admin.Serve(pCtx, adminAddr, admin.NewService(store, log), log)
		close(adminDone)
	}()
	t.Cleanup(func() { <-adminDone })

	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", proxyAddr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		c2, err := net.DialTimeout("tcp", adminAddr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c2.Close()
		return true
	}, 3*time.Second, 25*time.Millisecond, "proxy + admin should be up")

	client := admin.NewClient("http://" + adminAddr + "/rpc")
	require.NoError(t, client.Set(ctx, admin.SetParams{
		Username:    "carol",
		Password:    "carolpw",
		TenantID:    "tenantC",
		TopicPrefix: "tenantC.",
		Upstream:    upstream,
	}))

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed.Tenants, 1)
	assert.Equal(t, "carol", listed.Tenants[0].Username)

	prod := newClient(t, proxyAddr, "carol", "carolpw",
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic("admin-events"),
	)
	defer prod.Close()
	require.NoError(t, prod.ProduceSync(ctx,
		&kgo.Record{Value: []byte("hello-from-carol")}).FirstErr())

	// Verify the topic landed under the prefix on the broker.
	direct, err := kgo.NewClient(
		kgo.SeedBrokers(upstream),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	require.NoError(t, err)
	defer direct.Close()

	mdReq := kmsg.NewPtrMetadataRequest()
	mdResp, err := mdReq.RequestWith(ctx, direct)
	require.NoError(t, err)
	var found bool
	for _, top := range mdResp.Topics {
		if top.Topic != nil && *top.Topic == "tenantC.admin-events" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected tenantC.admin-events on the broker")
}
