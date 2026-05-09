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
// authenticate as "tenantA", "tenantB", "carol".
const integrationJAAS = `KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="broker"
    password="brokerpw"
    user_broker="brokerpw"
    user_tenantA="tenantA"
    user_tenantB="tenantB"
    user_carol="carolpw";
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
				"KAFKA_SASL_ENABLED_MECHANISMS":                  "PLAIN",
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
exec /etc/kafka/docker/run
`, host, hostPort.Port())
		return c.CopyToContainer(ctx, []byte(script), starterPath, 0o755)
	}
}
