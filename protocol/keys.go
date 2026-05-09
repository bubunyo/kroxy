package protocol

// API key constants for the subset the proxy interacts with directly.
// Other keys are forwarded as opaque bytes; we only need to know flexibility.
const (
	ProduceKey            int16 = 0
	FetchKey              int16 = 1
	ListOffsetsKey        int16 = 2
	MetadataKey           int16 = 3
	OffsetCommitKey       int16 = 8
	OffsetFetchKey        int16 = 9
	FindCoordinatorKey    int16 = 10
	JoinGroupKey          int16 = 11
	HeartbeatKey          int16 = 12
	LeaveGroupKey         int16 = 13
	SyncGroupKey          int16 = 14
	DescribeGroupsKey     int16 = 15
	ListGroupsKey         int16 = 16
	SaslHandshakeKey      int16 = 17
	ApiVersionsKey        int16 = 18
	CreateTopicsKey       int16 = 19
	DeleteTopicsKey       int16 = 20
	InitProducerIDKey     int16 = 22
	OffsetForLeaderEpoch  int16 = 23
	AddPartitionsToTxnKey int16 = 24
	AddOffsetsToTxnKey    int16 = 25
	EndTxnKey             int16 = 26
	TxnOffsetCommitKey    int16 = 28
	DescribeConfigsKey    int16 = 32
	SaslAuthenticateKey   int16 = 36
	DeleteGroupsKey       int16 = 42
	OffsetDeleteKey       int16 = 47
)

// isFlexibleRequest reports whether (apiKey, apiVersion) uses flexible
// (KIP-482) request encoding. Pulled from the Kafka protocol message
// definitions; only entries the proxy needs are listed. Unknown keys default
// to non-flexible which is a safe choice for byte-passthrough since we only
// need this for header parsing of keys we actually decode.
func isFlexibleRequest(apiKey, apiVersion int16) bool {
	switch apiKey {
	case ProduceKey:
		return apiVersion >= 9
	case FetchKey:
		return apiVersion >= 12
	case ListOffsetsKey:
		return apiVersion >= 6
	case MetadataKey:
		return apiVersion >= 9
	case OffsetCommitKey:
		return apiVersion >= 8
	case OffsetFetchKey:
		return apiVersion >= 6
	case FindCoordinatorKey:
		return apiVersion >= 3
	case JoinGroupKey:
		return apiVersion >= 6
	case HeartbeatKey:
		return apiVersion >= 4
	case LeaveGroupKey:
		return apiVersion >= 4
	case SyncGroupKey:
		return apiVersion >= 4
	case DescribeGroupsKey:
		return apiVersion >= 5
	case ListGroupsKey:
		return apiVersion >= 3
	case SaslHandshakeKey:
		return false
	case ApiVersionsKey:
		return apiVersion >= 3
	case CreateTopicsKey:
		return apiVersion >= 5
	case DeleteTopicsKey:
		return apiVersion >= 4
	case InitProducerIDKey:
		return apiVersion >= 2
	case AddPartitionsToTxnKey:
		return apiVersion >= 3
	case AddOffsetsToTxnKey:
		return apiVersion >= 3
	case EndTxnKey:
		return apiVersion >= 3
	case TxnOffsetCommitKey:
		return apiVersion >= 3
	case DescribeConfigsKey:
		return false
	case SaslAuthenticateKey:
		return apiVersion >= 2
	case DeleteGroupsKey:
		return apiVersion >= 2
	case OffsetDeleteKey:
		return false
	}
	return false
}
