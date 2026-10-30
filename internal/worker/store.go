package worker

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"

	"agentlink/internal/seal"
	"agentlink/internal/sqlite"
)

// store keeps the worker's state in SQLite as JSON values boxed with a local storage key,
// each under its own key as additional data so a value cannot be moved to another key.
type store struct {
	db  *sql.DB
	key []byte
}

func openStore(dir string) (*store, error) {
	key, err := seal.LoadOrCreateKey(filepath.Join(dir, "worker.key"))
	if err != nil {
		return nil, err
	}
	db, err := sqlite.Open(filepath.Join(dir, "worker.db"), `CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v BLOB NOT NULL);`)
	if err != nil {
		return nil, err
	}
	return &store{db: db, key: key}, nil
}

func (st *store) get(k string, v any) (bool, error) {
	var boxed []byte
	err := st.db.QueryRow(`SELECT v FROM kv WHERE k = ?`, k).Scan(&boxed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, st.decode(k, boxed, v)
}

func (st *store) put(k string, v any) error {
	plain, err := json.Marshal(v)
	if err != nil {
		return err
	}
	boxed, err := seal.Box(st.key, k, plain)
	if err != nil {
		return err
	}
	_, err = st.db.Exec(`INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v`, k, boxed)
	return err
}

func (st *store) decode(k string, boxed []byte, v any) error {
	plain, err := seal.Unbox(st.key, k, boxed)
	if err != nil {
		return errors.New(k + ": " + err.Error())
	}
	return json.Unmarshal(plain, v)
}

func (st *store) rooms() (map[string]*RoomState, error) {
	rows, err := st.db.Query(`SELECT k, v FROM kv WHERE k LIKE 'room/%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*RoomState{}
	for rows.Next() {
		var k string
		var boxed []byte
		if err := rows.Scan(&k, &boxed); err != nil {
			return nil, err
		}
		rs := &RoomState{}
		if err := st.decode(k, boxed, rs); err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(k, "room/")] = rs
	}
	return out, rows.Err()
}
