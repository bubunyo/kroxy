// Package rewrite implements per-API-key topic-name (and group-id) rewriting
// between proxy clients and the shared upstream Kafka cluster.
//
// All rewriting is symmetric: requests have the tenant prefix prepended on
// the way out; responses have the prefix stripped on the way back. Metadata
// and ListGroups responses additionally drop entries that do not belong to
// the tenant, so each tenant only sees its own slice of the cluster.
package rewrite

import "strings"

// PrefixIn prepends prefix to name.
func PrefixIn(prefix, name string) string {
	if prefix == "" || name == "" {
		return name
	}
	return prefix + name
}

// PrefixInPtr is the *string variant for nullable Kafka string fields.
func PrefixInPtr(prefix string, name *string) *string {
	if name == nil || *name == "" || prefix == "" {
		return name
	}
	out := prefix + *name
	return &out
}

// StripOut removes prefix from name. If name does not have prefix it is
// returned unchanged; this is intentional so unrecognised topics surface
// to the client with their upstream name (which the client can then choose
// to reject).
func StripOut(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return strings.TrimPrefix(name, prefix)
}

// StripOutPtr is the *string variant for nullable Kafka string fields.
func StripOutPtr(prefix string, name *string) *string {
	if name == nil || prefix == "" {
		return name
	}
	out := strings.TrimPrefix(*name, prefix)
	return &out
}

// BelongsToTenant reports whether name is owned by the tenant identified by
// prefix. The empty prefix matches everything (used during construction
// errors / tests).
func BelongsToTenant(prefix, name string) bool {
	if prefix == "" {
		return true
	}
	return strings.HasPrefix(name, prefix)
}
