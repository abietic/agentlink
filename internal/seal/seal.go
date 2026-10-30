// Package seal encrypts what agentlink stores and what it sends between server and worker.
//
// Box and Unbox protect data at rest with a local 32-byte storage key (AES-256-GCM).
//
// Seal and Open protect a payload end to end between two X25519 keys. The message key
// comes from an ephemeral-static and a static-static exchange (the shape of HPKE's auth
// mode), so only the recipient can open a box and only the sender's key can have made
// it. The context, e.g. "task dev-x t-y", is bound in as additional data: a box made for
// one device or task does not open as another.
package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const KeySize = 32

var ErrOpen = errors.New("seal: cannot decrypt (wrong key, context or tampered data)")

func aead(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Box encrypts plain under a storage key; the result is nonce || ciphertext.
func Box(key []byte, aad string, plain []byte) ([]byte, error) {
	a, err := aead(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, a.NonceSize())
	rand.Read(nonce)
	return a.Seal(nonce, nonce, plain, []byte(aad)), nil
}

func Unbox(key []byte, aad string, box []byte) ([]byte, error) {
	a, err := aead(key)
	if err != nil {
		return nil, err
	}
	if len(box) < a.NonceSize() {
		return nil, ErrOpen
	}
	out, err := a.Open(nil, box[:a.NonceSize()], box[a.NonceSize():], []byte(aad))
	if err != nil {
		return nil, ErrOpen
	}
	return out, nil
}

func NewPrivateKey() *ecdh.PrivateKey {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return k
}

func ParsePrivateKey(b []byte) (*ecdh.PrivateKey, error) { return ecdh.X25519().NewPrivateKey(b) }
func ParsePublicKey(b []byte) (*ecdh.PublicKey, error)   { return ecdh.X25519().NewPublicKey(b) }

// Seal encrypts msg for recipient; the result is ephemeral public key || nonce || ciphertext.
func Seal(sender *ecdh.PrivateKey, recipient *ecdh.PublicKey, context string, msg []byte) ([]byte, error) {
	eph := NewPrivateKey()
	dh1, err := eph.ECDH(recipient)
	if err != nil {
		return nil, err
	}
	dh2, err := sender.ECDH(recipient)
	if err != nil {
		return nil, err
	}
	key, err := derive(dh1, dh2, eph.PublicKey(), sender.PublicKey(), recipient, context)
	if err != nil {
		return nil, err
	}
	box, err := Box(key, context, msg)
	if err != nil {
		return nil, err
	}
	return append(eph.PublicKey().Bytes(), box...), nil
}

// Open decrypts a box that sender sealed for recipient under the same context.
func Open(recipient *ecdh.PrivateKey, sender *ecdh.PublicKey, context string, box []byte) ([]byte, error) {
	if len(box) < KeySize {
		return nil, ErrOpen
	}
	eph, err := ParsePublicKey(box[:KeySize])
	if err != nil {
		return nil, ErrOpen
	}
	dh1, err := recipient.ECDH(eph)
	if err != nil {
		return nil, ErrOpen
	}
	dh2, err := recipient.ECDH(sender)
	if err != nil {
		return nil, ErrOpen
	}
	key, err := derive(dh1, dh2, eph, sender, recipient.PublicKey(), context)
	if err != nil {
		return nil, err
	}
	return Unbox(key, context, box[KeySize:])
}

func derive(dh1, dh2 []byte, eph, sender, recipient *ecdh.PublicKey, context string) ([]byte, error) {
	salt := append(append(eph.Bytes(), sender.Bytes()...), recipient.Bytes()...)
	return hkdf.Key(sha256.New, append(dh1, dh2...), salt, "agentlink/v1 "+context, KeySize)
}

// LoadOrCreateKey reads a base64 storage key from path, creating it (mode 0600) on first use.
func LoadOrCreateKey(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		key := make([]byte, KeySize)
		rand.Read(key)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, err
		}
		_, err = f.WriteString(base64.StdEncoding.EncodeToString(key) + "\n")
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		return key, err
	}
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(key) != KeySize {
		return nil, fmt.Errorf("%s: not a base64 %d-byte key", path, KeySize)
	}
	return key, nil
}
