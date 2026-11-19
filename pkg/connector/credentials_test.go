package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewFromJSONBytesRejectsUntrustedConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "subject-token")
	if err := os.WriteFile(path, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data string
	}{
		{"external file source", `{"type":"external_account","credential_source":{"file":"` + path + `"}}`},
		{"external URL source", `{"type":"external_account","credential_source":{"url":"https://example.invalid/token"}}`},
		{"authorized user", `{"type":"authorized_user","refresh_token":"sensitive"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewFromJSONBytes(context.Background(), []byte(tc.data))
			if client != nil {
				client.ProjectsClient.Close()
				client.BigQueryClient.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "google credential:") {
				t.Fatalf("expected credential rejection, got client=%v err=%v", client, err)
			}
			if strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("credential error leaked input: %v", err)
			}
		})
	}
}

func TestNewFromJSONBytesAcceptsOlderConsoleKey(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	// #nosec G101 -- this private key is generated only for the test.
	data, err := json.Marshal(map[string]string{
		"type": "service_account", "project_id": "example-project", "private_key_id": "test-key",
		"private_key":  string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
		"client_email": "test@example-project.iam.gserviceaccount.com",
		"token_uri":    "https://accounts.google.com/o/oauth2/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewFromJSONBytes(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.ProjectsClient.Close()
		client.BigQueryClient.Close()
	})
}

func TestNewRejectsOversizedCredentialFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", (64<<10)+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := New(context.Background(), path)
	if client != nil || err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("expected size rejection, got client=%v err=%v", client, err)
	}
}
