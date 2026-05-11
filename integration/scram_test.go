//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// newSCRAMClient builds a kgo.Client that authenticates via SCRAM. The
// mechanism is selected from the digest size returned by h: SHA-256 or
// SHA-512.
func newSCRAMClient(t *testing.T, addr, user, pw string, h func() hash.Hash, extra ...kgo.Opt) *kgo.Client {
	t.Helper()
	a := scram.Auth{User: user, Pass: pw}
	var saslOpt kgo.Opt
	switch h().Size() {
	case sha256.Size:
		saslOpt = kgo.SASL(a.AsSha256Mechanism())
	case sha512.Size:
		saslOpt = kgo.SASL(a.AsSha512Mechanism())
	default:
		t.Fatalf("unsupported hash size %d", h().Size())
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		saslOpt,
		kgo.RequestTimeoutOverhead(15 * time.Second),
		kgo.MetadataMinAge(100 * time.Millisecond),
	}
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	return cl
}

// TestEndToEnd_SCRAMSHA256 verifies a full produce/consume round-trip
// through kroxy using SCRAM-SHA-256 against the upstream broker, which is
// the sole authentication authority.
func TestEndToEnd_SCRAMSHA256(t *testing.T) {
	runSCRAMEndToEnd(t, sha256.New, "scram256-events")
}

// TestEndToEnd_SCRAMSHA512 mirrors the SHA-256 test for SHA-512.
func TestEndToEnd_SCRAMSHA512(t *testing.T) {
	runSCRAMEndToEnd(t, sha512.New, "scram512-events")
}

func runSCRAMEndToEnd(t *testing.T, h func() hash.Hash, topic string) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	upstream, stopK := startKafkaSASL(ctx, t)
	t.Cleanup(stopK)

	proxyAddr, stop := startProxy(t, upstream)
	defer stop()

	prod := newSCRAMClient(t, proxyAddr, tenantA, tenantAPw, h,
		kgo.AllowAutoTopicCreation(),
		kgo.DefaultProduceTopic(topic),
	)
	defer prod.Close()

	const recordCount = 5
	pCtx, pCancel := context.WithTimeout(ctx, 60*time.Second)
	defer pCancel()
	for i := 0; i < recordCount; i++ {
		r := &kgo.Record{Value: []byte(fmt.Sprintf("scram-%d", i))}
		results := prod.ProduceSync(pCtx, r)
		require.NoError(t, results.FirstErr(), "produce failed at %d", i)
	}

	cons := newSCRAMClient(t, proxyAddr, tenantA, tenantAPw, h,
		kgo.ConsumerGroup(topic+"-readers"),
		kgo.ConsumeTopics(topic),
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
			assert.Equal(t, topic, r.Topic)
		})
	}

	want := make([]string, recordCount)
	for i := range want {
		want[i] = fmt.Sprintf("scram-%d", i)
	}
	assert.ElementsMatch(t, want, got)

	// Verify the broker stored the prefixed topic name.
	direct, err := kgo.NewClient(
		kgo.SeedBrokers(upstream),
		kgo.SASL(plain.Auth{User: "broker", Pass: "brokerpw"}.AsMechanism()),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	require.NoError(t, err)
	defer direct.Close()

	mdReq := kmsg.NewPtrMetadataRequest()
	mdResp, err := mdReq.RequestWith(ctx, direct)
	require.NoError(t, err)

	var found bool
	for _, top := range mdResp.Topics {
		if top.Topic != nil && *top.Topic == tenantA+"."+topic {
			found = true
			break
		}
	}
	assert.True(t, found, "expected %s.%s on the broker", tenantA, topic)
}
