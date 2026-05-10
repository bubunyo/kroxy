//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// JAAS file declaring the SASL/PLAIN principals the test broker accepts.
//
// Inter-broker / admin uses "broker" / "brokerpw"; integration tests
// authenticate as "tenantA", "tenantB", "carol" via PLAIN. SCRAM-SHA-256
// and SCRAM-SHA-512 credentials for the same usernames are bootstrapped at
// storage-format time via kafka-storage.sh --add-scram (see starter script
// in copyStarterScript). SCRAM principals do not appear in this JAAS file.
const integrationJAAS = `KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="broker"
    password="brokerpw"
    user_broker="brokerpw"
    user_tenantA="tenantA"
    user_tenantB="tenantB"
    user_carol="carolpw";
    org.apache.kafka.common.security.scram.ScramLoginModule required;
};
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="broker"
    password="brokerpw";
};
`

const starterPath = "/usr/local/bin/kroxy_start.sh"

// startKafkaSASL launches an apache/kafka KRaft container with a
// SASL_PLAINTEXT listener and returns the host:port external clients (and
// kroxy) should dial plus a cleanup func.
//
// The container entrypoint sits in a wait loop until a starter script is
// copied in by a PostStart hook; the script discovers the host-mapped port
// (so KAFKA_ADVERTISED_LISTENERS resolves correctly from outside the
// container) and execs the apache/kafka entrypoint.
func startKafkaSASL(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()

	const externalPort = "9093/tcp"

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:3.8.0",
			ExposedPorts: []string{externalPort},
			Env: map[string]string{
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_LISTENERS":                                "SASL_PLAINTEXT://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "SASL_PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
				"KAFKA_SASL_ENABLED_MECHANISMS":                  "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512",
				"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL":     "PLAIN",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
				"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "true",
				"KAFKA_OPTS":                                     "-Djava.security.auth.login.config=/etc/kafka/kafka_jaas.conf",
				"CLUSTER_ID":                                     "4L6g3nShT-eMCtK--X86sw",
			},
			Files: []testcontainers.ContainerFile{
				{
					ContainerFilePath: "/etc/kafka/kafka_jaas.conf",
					FileMode:          0o644,
					Reader:            strings.NewReader(integrationJAAS),
				},
			},
			Entrypoint: []string{"sh", "-c", "while [ ! -f " + starterPath + " ]; do sleep 0.05; done; bash " + starterPath},
			LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{
				PostStarts: []testcontainers.ContainerHook{copyStarterScript(externalPort)},
			}},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	}

	c, err := testcontainers.GenericContainer(ctx, req)
	require.NoError(t, err)

	hostPort, err := c.MappedPort(ctx, externalPort)
	require.NoError(t, err)
	host, err := c.Host(ctx)
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", host, hostPort.Port())
	cleanup := func() { _ = c.Terminate(context.Background()) }
	return addr, cleanup
}

// copyStarterScript returns a PostStart hook that discovers the
// host-mapped external port and writes the starter script into the
// container; the entrypoint's wait loop then unblocks.
func copyStarterScript(externalPort string) testcontainers.ContainerHook {
	return func(ctx context.Context, c testcontainers.Container) error {
		if err := wait.ForMappedPort(externalPort).WaitUntilReady(ctx, c); err != nil {
			return fmt.Errorf("wait for mapped port: %w", err)
		}
		hostPort, err := c.MappedPort(ctx, externalPort)
		if err != nil {
			return fmt.Errorf("mapped port: %w", err)
		}
		host, err := c.Host(ctx)
		if err != nil {
			return fmt.Errorf("host: %w", err)
		}
		script := fmt.Sprintf(`#!/bin/bash
set -e
export KAFKA_ADVERTISED_LISTENERS="SASL_PLAINTEXT://%s:%s"

# Pre-format storage with SCRAM credentials for the SASL principals the
# tests need. The credentials use the same passwords declared in JAAS for
# PLAIN so a single password works for both mechanisms. The official
# docker wrapper (invoked by /etc/kafka/docker/run) will see the storage
# is "already formatted" and proceed without overwriting.
LOG_DIRS=$(awk -F= '/^log.dirs/ {print $2}' /etc/kafka/docker/server.properties || true)
LOG_DIRS=${LOG_DIRS:-/tmp/kraft-combined-logs}
mkdir -p "$LOG_DIRS"
KAFKA_HEAP_OPTS="-Xms64M -Xmx128M" /opt/kafka/bin/kafka-storage.sh format \
    --cluster-id "$CLUSTER_ID" \
    --config /etc/kafka/docker/server.properties \
    --add-scram 'SCRAM-SHA-256=[name=tenantA,password=tenantA]' \
    --add-scram 'SCRAM-SHA-512=[name=tenantA,password=tenantA]' \
    --add-scram 'SCRAM-SHA-256=[name=tenantB,password=tenantB]' \
    --add-scram 'SCRAM-SHA-512=[name=tenantB,password=tenantB]' \
    --add-scram 'SCRAM-SHA-256=[name=carol,password=carolpw]' \
    --add-scram 'SCRAM-SHA-512=[name=carol,password=carolpw]' \
    --ignore-formatted || true

exec /etc/kafka/docker/run
`, host, hostPort.Port())
		return c.CopyToContainer(ctx, []byte(script), starterPath, 0o755)
	}
}
