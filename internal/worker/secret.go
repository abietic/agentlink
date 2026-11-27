package worker

import "fmt"

// secretKeyTanka is where the Tanka signing secret lives in worker.db. It is not in
// worker.json: the store boxes it with worker.key, which only this device holds.
const secretKeyTanka = "tanka_secret"

// SetSecret stores the named secret in this worker's encrypted database, or removes it when
// secret is empty. The only name today is "tanka", the Tanka app's signing secret. It never
// logs, prints or returns the value.
func SetSecret(cfg Config, name string, secret []byte) error {
	if name != "tanka" {
		return fmt.Errorf("unknown secret %q: the only one is \"tanka\"", name)
	}
	st, err := openStore(cfg.StateDir)
	if err != nil {
		return err
	}
	defer st.Close()
	if len(secret) == 0 {
		return st.delete(secretKeyTanka)
	}
	return st.put(secretKeyTanka, secret)
}
