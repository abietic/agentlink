package worker

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"

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

// storedRoom is a room's state as stored. The kv key is derived from the room id (roomKey)
// so the database does not reveal which rooms this device serves; the id lives in the box.
type storedRoom struct {
	RoomID string     `json:"room_id"`
	State  *RoomState `json:"state"`
}

func (st *store) roomKey(roomID string) string {
	m := hmac.New(sha256.New, st.key)
	m.Write([]byte("agentlink/v1 room " + roomID))
	return "room/" + hex.EncodeToString(m.Sum(nil)[:16])
}

func (st *store) putRoom(roomID string, rs *RoomState) error {
	return st.put(st.roomKey(roomID), storedRoom{RoomID: roomID, State: rs})
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
		var sr storedRoom
		if err := st.decode(k, boxed, &sr); err != nil {
			return nil, err
		}
		if sr.RoomID == "" || sr.State == nil {
			continue
		}
		out[sr.RoomID] = sr.State
	}
	return out, rows.Err()
}
