// Package admin exposes a JSON-RPC 2.0 service for managing kroxy tenants
// at runtime.
//
// The service is intentionally unauthenticated; bind it to a loopback
// address or otherwise gate access at the network layer until v2 introduces
// a proper auth model.
package admin

// SetParams is the payload for the Tenants.Set method. All fields are
// required.
//
// kroxy stores no secrets: the tenant ID is the SASL/PLAIN identity
// expected on the wire and the password supplied by the client is forwarded
// verbatim to the tenant's upstream Kafka cluster.
type SetParams struct {
	ID          string `json:"id"`
	TopicPrefix string `json:"topic_prefix"`
	Upstream    string `json:"upstream"`
}

// DeleteParams is the payload for the Tenants.Delete method.
type DeleteParams struct {
	ID string `json:"id"`
}

// OKResult is returned by mutating methods on success.
type OKResult struct {
	OK bool `json:"ok"`
}

// TenantView is a description of a tenant returned by Tenants.List.
type TenantView struct {
	ID          string `json:"id"`
	TopicPrefix string `json:"topic_prefix"`
	Upstream    string `json:"upstream"`
}

// ListResult is returned by Tenants.List.
type ListResult struct {
	Tenants []TenantView `json:"tenants"`
}
