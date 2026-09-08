package job

import "encoding/json"

// Secret is a string that refuses to reveal itself through the channels values
// normally leak from: fmt verbs, JSON encoding, and log lines built with %v.
//
// This is the cheap half of "inject credentials without them being logged". It
// does not stop Reveal() being called and the result logged — nothing can — but
// it removes the accidental path, which is how credentials actually escape in
// practice: a struct dumped into a log line during debugging, and nobody noticed.
//
// The expensive half lives at the boundary: secrets are resolved from a store at
// dispatch time, passed to the environment out of band, and never written into the
// job record that we persist and serve over the API.
type Secret string

const redacted = "[REDACTED]"

// String implements fmt.Stringer, covering %s, %v and print-family calls.
func (s Secret) String() string { return redacted }

// GoString covers %#v, which ignores String().
func (s Secret) GoString() string { return redacted }

// MarshalJSON covers encoding/json, and so every JSON logger and API response.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redacted) }

// Reveal returns the real value. Every call site is a place to justify in review,
// which is exactly the point: grep for Reveal( to audit the blast radius.
func (s Secret) Reveal() string { return string(s) }

// SecretRef names a secret in an external store rather than carrying its value.
// Job specs use refs, not values: the persisted job record must be safe to store,
// serve over the API, and paste into a bug report.
type SecretRef struct {
	// Name is the environment variable the agent will see.
	Name string `json:"name"`
	// Source identifies the backing store, e.g. "env", "file", "aws-secrets-manager".
	Source string `json:"source"`
	// Key is the lookup key within that source.
	Key string `json:"key"`
}
