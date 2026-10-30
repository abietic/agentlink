package seal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSealOpen(t *testing.T) {
	server, device, other := NewPrivateKey(), NewPrivateKey(), NewPrivateKey()
	box, err := Seal(server, device.PublicKey(), "task dev-a t-1", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(device, server.PublicKey(), "task dev-a t-1", box)
	if err != nil || string(got) != "hello" {
		t.Fatalf("open: %q, %v", got, err)
	}
	tampered := bytes.Clone(box)
	tampered[len(tampered)-1] ^= 1
	for name, try := range map[string]func() ([]byte, error){
		"other context":   func() ([]byte, error) { return Open(device, server.PublicKey(), "task dev-b t-1", box) },
		"other recipient": func() ([]byte, error) { return Open(other, server.PublicKey(), "task dev-a t-1", box) },
		"other sender":    func() ([]byte, error) { return Open(device, other.PublicKey(), "task dev-a t-1", box) },
		"tampered":        func() ([]byte, error) { return Open(device, server.PublicKey(), "task dev-a t-1", tampered) },
		"truncated":       func() ([]byte, error) { return Open(device, server.PublicKey(), "task dev-a t-1", box[:20]) },
	} {
		if _, err := try(); err == nil {
			t.Errorf("%s: opened", name)
		}
	}
}

func TestBoxAndKeyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "master.key")
	key, err := LoadOrCreateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("key file mode %v", fi.Mode().Perm())
	}
	again, err := LoadOrCreateKey(path)
	if err != nil || !bytes.Equal(key, again) {
		t.Fatalf("reload: %v", err)
	}
	box, _ := Box(key, "a", []byte("secret"))
	if bytes.Contains(box, []byte("secret")) {
		t.Fatal("plaintext visible")
	}
	if got, err := Unbox(key, "a", box); err != nil || string(got) != "secret" {
		t.Fatalf("unbox: %q %v", got, err)
	}
	if _, err := Unbox(key, "b", box); err == nil {
		t.Fatal("unboxed under another aad")
	}
}
