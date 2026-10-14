package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/pkg/errors"
)

var (
	// timeout is the timeout for webhook request. Default to 30 seconds.
	timeout = 30 * time.Second

	// safeClient is the shared HTTP client used for all webhook dispatches.
	// Its Transport guards against SSRF by blocking connections to reserved/private
	// IP addresses at dial time, which also defeats DNS rebinding attacks.
	safeClient = &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: safeDialContext,
		},
	}
)

// safeDialContext is a net.Dialer.DialContext replacement that resolves the target
// hostname, rejects disallowed reserved/private addresses, and dials an already
// checked IP address so DNS cannot be rebound between validation and connection.
func safeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.Errorf("webhook: invalid address %q", addr)
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "webhook: failed to resolve host %q", host)
	}

	validIPs := make([]net.IPAddr, 0, len(ips))
	for _, ip := range ips {
		if parsed, ok := netip.AddrFromSlice(ip.IP); !ok || !parsed.IsValid() {
			continue
		}
		if isBlockedDestination(host, ip.IP) {
			return nil, errors.Errorf("webhook: connection to reserved/private IP address is not allowed")
		}
		validIPs = append(validIPs, ip)
	}

	var dialErr error
	dialer := &net.Dialer{}
	for _, ip := range validIPs {
		dialHost := ip.IP.String()
		if ip.Zone != "" {
			dialHost += "%" + ip.Zone
		}
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(dialHost, port))
		if err == nil {
			return conn, nil
		}
		dialErr = err
	}
	if dialErr != nil {
		return nil, errors.Wrapf(dialErr, "webhook: failed to connect to host %q", host)
	}
	return nil, errors.Errorf("webhook: host %q resolved to no IP addresses", host)
}

// Request is one webhook delivery: Payload is JSON-encoded and posted to URL,
// signed with SigningSecret when one is set.
type Request struct {
	// URL is the destination endpoint.
	URL string
	// Label identifies the delivery in logs, for example an activity type.
	Label string
	// SigningSecret enables Standard Webhooks HMAC-SHA256 signing when non-empty.
	SigningSecret string
	// Payload is the value encoded as the JSON request body.
	Payload any
	// MessageID, when set, is sent as the Standard Webhooks webhook-id header.
	// The persistent queue sets it once per logical delivery and reuses it across
	// retries so receivers can deduplicate redeliveries. When empty, Deliver
	// generates a fresh id for the attempt.
	MessageID string
}

// AttemptResult describes the HTTP response of one send attempt.
type AttemptResult struct {
	// StatusCode is the HTTP status code, or 0 when the request never reached
	// the receiver (DNS, dial, timeout, or transport error).
	StatusCode int
}

// statusCodeRetryable reports whether an HTTP status warrants another attempt.
// 5xx responses are transient server failures. 408 (request timeout) and 429
// (rate limited) are explicit 4xx codes whose semantics demand a retry; every
// other 4xx is a definitive rejection that retrying cannot fix.
func statusCodeRetryable(statusCode int) bool {
	if statusCode >= 500 && statusCode <= 599 {
		return true
	}
	return statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests
}

// transportErrorRetryable classifies a failed http.Client.Do. Timeouts, DNS
// failures, refused/reset connections and shutdown cancellations may clear on
// their own; an SSRF rejection is configuration, so retrying cannot help.
func transportErrorRetryable(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "reserved/private IP address is not allowed") {
		return false
	}
	return true
}

// resolveSigningKey returns the raw HMAC key for a signing secret. Secrets using
// the Standard Webhooks "whsec_<base64>" serialization are base64-decoded to their
// raw bytes; any other secret is used as-is. It returns an error when a whsec_-prefixed
// secret is not valid base64, so callers fail loudly instead of silently signing with
// the wrong key (which would make every signature unverifiable by the receiver).
func resolveSigningKey(secret string) ([]byte, error) {
	if rest, ok := strings.CutPrefix(secret, "whsec_"); ok {
		decoded, err := base64.StdEncoding.DecodeString(rest)
		if err != nil {
			return nil, errors.Wrap(err, "signing secret has whsec_ prefix but is not valid base64")
		}
		return decoded, nil
	}
	return []byte(secret), nil
}

// GenerateSigningSecret returns a new Standard Webhooks signing secret in the
// "whsec_<base64>" form, backed by 32 cryptographically-random bytes — comfortably
// within the spec's 24–64 byte range.
func GenerateSigningSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.Wrap(err, "failed to read random bytes for signing secret")
	}
	return "whsec_" + base64.StdEncoding.EncodeToString(buf), nil
}

// Deliver performs one signed delivery attempt and classifies the outcome.
//
// A nil error means the receiver accepted the delivery. A non-nil error carries
// the retryable verdict: true for timeouts, transport failures and 5xx/408/429
// responses where another attempt may succeed; false for definitive failures
// such as an SSRF rejection, an invalid signing secret, and explicit 4xx
// responses. The returned *AttemptResult is nil when no HTTP response was
// received.
func Deliver(ctx context.Context, requestPayload *Request) (*AttemptResult, bool, error) {
	body, err := json.Marshal(requestPayload.Payload)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to marshal webhook request to %s", requestPayload.URL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestPayload.URL, bytes.NewBuffer(body))
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to construct webhook request to %s", requestPayload.URL)
	}

	req.Header.Set("Content-Type", "application/json")

	if requestPayload.SigningSecret != "" {
		key, err := resolveSigningKey(requestPayload.SigningSecret)
		if err != nil {
			return nil, false, errors.Wrapf(err, "failed to derive signing key for webhook to %s", requestPayload.URL)
		}

		messageID := requestPayload.MessageID
		if messageID == "" {
			messageID = "msg_" + uuid.NewV4().String()
		}
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)

		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(messageID + "." + timestamp + "."))
		mac.Write(body)
		signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

		req.Header.Set("webhook-id", messageID)
		req.Header.Set("webhook-timestamp", timestamp)
		req.Header.Set("webhook-signature", "v1,"+signature)
	}

	resp, err := safeClient.Do(req)
	if err != nil {
		return nil, transportErrorRetryable(err), errors.Wrapf(err, "failed to post webhook to %s", requestPayload.URL)
	}
	defer resp.Body.Close()

	result := &AttemptResult{StatusCode: resp.StatusCode}

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return result, true, errors.Wrapf(err, "failed to read webhook response from %s", requestPayload.URL)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if statusCodeRetryable(resp.StatusCode) {
			return result, true, errors.Errorf("webhook to %s returned retryable status code: %d", requestPayload.URL, resp.StatusCode)
		}
		return result, false, errors.Errorf("webhook to %s returned non-retryable status code: %d", requestPayload.URL, resp.StatusCode)
	}

	response := &struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{}
	if err := json.Unmarshal(responseBody, response); err != nil {
		return result, false, errors.Wrapf(err, "failed to unmarshal webhook response from %s", requestPayload.URL)
	}

	if response.Code != 0 {
		return result, false, errors.Errorf("receive error code sent by webhook server, code %d, msg: %s", response.Code, response.Message)
	}

	return result, false, nil
}

// Post delivers the request synchronously and returns the receiver's verdict.
// It is a thin compatibility wrapper over Deliver using the background context.
func Post(requestPayload *Request) error {
	_, _, err := Deliver(context.Background(), requestPayload)
	return err
}
