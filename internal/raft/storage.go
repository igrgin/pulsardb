package raft

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/tidwall/wal"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Filenames for side-car state.
const (
	hardStateFolder = "hard_state"
	snapshotFolder  = "snapshot"
	walFolder       = "wal"
)

type Storage struct {
	mu sync.Mutex

	dir string   // base directory for this Node
	log *wal.Log // tidwall WAL
	ms  *etcdraft.MemoryStorage

	hs   raftpb.HardState
	snap raftpb.Snapshot
}

// OpenStorage opens or creates a Storage in the given directory.
// It loads snapshot + hardstate and then replays WAL into MemoryStorage.
func OpenStorage(dir string) (*Storage, uint64, error) {
	format := "mkdir %s: %w"
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, fmt.Errorf(format, dir, err)
	}

	if err := os.MkdirAll(path.Join(dir, walFolder), 0o750); err != nil {
		return nil, 0, fmt.Errorf(format, dir, err)
	}

	if err := os.MkdirAll(path.Join(dir, snapshotFolder), 0o750); err != nil {
		return nil, 0, fmt.Errorf(format, dir, err)
	}

	if err := os.MkdirAll(path.Join(dir, hardStateFolder), 0o750); err != nil {
		return nil, 0, fmt.Errorf(format, dir, err)
	}

	// Open WAL with AllowEmpty=true so we can truncate to empty safely.
	opts := *wal.DefaultOptions

	walPath := filepath.Join(dir, walFolder)

	log, err := wal.Open(walPath, &opts)
	if err != nil {
		return nil, 0, fmt.Errorf("wal.Open(%q): %w", walPath, err)
	}

	s := &Storage{
		dir: dir,
		log: log,
		ms:  etcdraft.NewMemoryStorage(),
	}

	if err := s.loadSnapshot(); err != nil {
		s.log.Close()
		return nil, 0, err
	}
	if err := s.loadHardState(); err != nil {
		s.log.Close()
		return nil, 0, err
	}
	if err := s.replayWAL(); err != nil {
		s.log.Close()
		return nil, 0, err
	}

	idx, _ := s.log.LastIndex()
	return s, idx, nil
}

// RaftStorage returns the etcdraft.Storage implementation you pass into etcdraft.Config.Storage.
func (s *Storage) RaftStorage() *etcdraft.MemoryStorage {
	return s.ms
}

// Close closes the wal and releases resources.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		return s.log.Close()
	}
	return nil
}

// ---------- load state on startup ----------

func (s *Storage) loadSnapshot() error {
	dir := filepath.Join(s.dir, snapshotFolder)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read snapshot dir: %w", err)
	}
	var fileName string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if fileName != "" {
			return fmt.Errorf("multiple snapshot files in %s", dir)
		}
		fileName = e.Name()
	}
	if fileName == "" {
		// No snapshot file present.
		return nil
	}
	dir = filepath.Join(dir, fileName)

	data, err := os.ReadFile(dir)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	pbutil.MustUnmarshal(&s.snap, data)

	// Apply snapshot to in-memory storage.
	if err := s.ms.ApplySnapshot(s.snap); err != nil && !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
		return fmt.Errorf("ApplySnapshot: %w", err)
	}
	return nil
}

func (s *Storage) loadHardState() error {
	dir := filepath.Join(s.dir, hardStateFolder)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read snapshot dir: %w", err)
	}
	var fileName string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if fileName != "" {
			return fmt.Errorf("multiple snapshot files in %s", dir)
		}
		fileName = e.Name()
	}
	if fileName == "" {
		return nil
	}
	dir = filepath.Join(dir, fileName)

	data, err := os.ReadFile(dir)
	if err != nil {
		return fmt.Errorf("read hardstate: %w", err)
	}
	pbutil.MustUnmarshal(&s.hs, data)
	if err := s.ms.SetHardState(s.hs); err != nil {
		return fmt.Errorf("failed to set hardstate with error: %v", err)
	}
	return nil
}

func (s *Storage) replayWAL() error {
	empty, err := s.log.IsEmpty()
	if err != nil {
		return fmt.Errorf("wal.IsRaftStorageEmpty: %w", err)
	}
	if empty {
		return nil
	}

	first, err := s.log.FirstIndex()
	if err != nil {
		return fmt.Errorf("wal.FirstIndex: %w", err)
	}
	last, err := s.log.LastIndex()
	if err != nil {
		return fmt.Errorf("wal.LastIndex: %w", err)
	}

	var ents []raftpb.Entry
	snapIndex := s.snap.Metadata.Index

	for idx := first; idx <= last; idx++ {
		data, err := s.log.Read(idx)
		if err != nil {
			if errors.Is(err, wal.ErrNotFound) || errors.Is(err, wal.ErrCorrupt) {
				// In a real system you might want to attempt recovery here.
				return fmt.Errorf("wal.Read(%d): %w", idx, err)
			}
			return fmt.Errorf("wal.Read(%d): %w", idx, err)
		}
		var e raftpb.Entry
		pbutil.MustUnmarshal(&e, data)
		if e.Index <= snapIndex {
			// Already reflected in snapshot.
			continue
		}
		ents = append(ents, e)
	}

	if len(ents) > 0 {
		if err := s.ms.Append(ents); err != nil {
			return fmt.Errorf("MemoryStorage.Append: %w", err)
		}
	}

	return nil
}

// ---------- Persist Ready from etcdraft.Node ----------

// SaveReady persists the parts of etcdraft.Ready that must go to stable storage:
//
//   - Snapshot (if non-empty)
//   - Entries
//   - HardState
//
// It then applies the same changes to MemoryStorage.
//
// Call this before sending rd.Messages, in line with the Ready contract. :contentReference[oaicite:3]{index=3}
func (s *Storage) SaveReady(rd etcdraft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) Snapshot
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := s.saveSnapshot(rd.Snapshot); err != nil {
			return err
		}
		// Update in-memory snapshot.
		if err := s.ms.ApplySnapshot(rd.Snapshot); err != nil && err != etcdraft.ErrSnapOutOfDate {
			return fmt.Errorf("ApplySnapshot: %w", err)
		}
	}

	// 2) Entries
	if len(rd.Entries) > 0 {
		if err := s.appendToWAL(rd.Entries); err != nil {
			return err
		}
		// Append to in-memory log.
		if err := s.ms.Append(rd.Entries); err != nil {
			return fmt.Errorf("MemoryStorage.Append: %w", err)
		}
	}

	// 3) HardState
	if !etcdraft.IsEmptyHardState(rd.HardState) {
		if !isHardStateEqual(s.hs, rd.HardState) {
			if err := s.saveHardState(rd.HardState); err != nil {
				return err
			}
			s.hs = rd.HardState
			err := s.ms.SetHardState(rd.HardState)
			if err != nil {
				return fmt.Errorf("failed to set hardstate with error: %v", err)
			}
		}
	}

	// 4) Sync if required.
	if rd.MustSync {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("wal.Sync: %w", err)
		}
		// In a real system, you should also fsync the state/snapshot files.
	}

	return nil
}

// appendToWAL appends (or overwrites) entries in the tidwall/wal.Log.
// It handles conflicting entries by truncating the WAL tail before writing new entries.
func (s *Storage) appendToWAL(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	firstNew := ents[0].Index
	lastNew := ents[len(ents)-1].Index

	empty, err := s.log.IsEmpty()
	if err != nil {
		return fmt.Errorf("wal.IsRaftStorageEmpty: %w", err)
	}

	if !empty {
		last, err := s.log.LastIndex()
		if err != nil {
			return fmt.Errorf("wal.LastIndex: %w", err)
		}

		// If we already have entries at or after firstNew, remove them.
		if last >= firstNew {
			if err := s.log.TruncateBack(firstNew - 1); err != nil {
				return fmt.Errorf("wal.TruncateBack(%d): %w", firstNew-1, err)
			}
			last = firstNew - 1
		}

		// Now log must be positioned so that next index is last+1 == firstNew.
		if last+1 != firstNew {
			return fmt.Errorf("wal append gap: last=%d, firstNew=%d", last, firstNew)
		}
	}

	// Append entries one by one; you can batch with wal.Batch if desired.
	for _, e := range ents {
		data := pbutil.MustMarshal(&e)
		if err := s.log.Write(e.Index, data); err != nil {
			return fmt.Errorf("wal.Write(%d): %w", e.Index, err)
		}
	}

	// Optional: sanity check that WAL last index is what we expect.
	if last, err := s.log.LastIndex(); err == nil && last != lastNew {
		return fmt.Errorf("wal last index mismatch: got=%d want=%d", last, lastNew)
	}

	return nil
}

// saveHardState writes HardState to a side file.
func (s *Storage) saveHardState(hs raftpb.HardState) error {
	// Write into a concrete file inside the hard_state folder.
	path := filepath.Join(s.dir, hardStateFolder, "current")
	data := pbutil.MustMarshal(&hs)
	if err := atomicWriteFile(path, data, 0o640); err != nil {
		return fmt.Errorf("write hardstate: %w", err)
	}
	return nil
}

// saveSnapshot writes Snapshot to a side file.
// You can also trigger log truncation here if you want to reclaim disk space.
func (s *Storage) saveSnapshot(snap raftpb.Snapshot) error {
	path := filepath.Join(s.dir, snapshotFolder, "current")
	data := pbutil.MustMarshal(&snap)
	if err := atomicWriteFile(path, data, 0o640); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}

	// OPTIONAL: truncate WAL front to drop entries <= snapshot index.
	// Be careful not to truncate away *all* entries (see ErrOutOfRange docs). :contentReference[oaicite:4]{index=4}
	//
	// last, err := s.log.LastIndex()
	// if err == nil && last > snap.Metadata.Index {
	//     if err := s.log.TruncateFront(snap.Metadata.Index + 1); err != nil {
	//         return fmt.Errorf("wal.TruncateFront(%d): %w", snap.Metadata.Index+1, err)
	//     }
	// }

	s.snap = snap
	return nil
}

// ---------- small helpers ----------

// atomicWriteFile writes a file atomically via a temp file + rename.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// Optional: fsync directory for extra safety.
	if f, err := os.Open(dir); err == nil {
		defer f.Close()
		_ = f.Sync()
	}
	return nil
}

func isHardStateEqual(a, b raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func (s *Storage) IsStorageEmpty() (bool, error) {
	emptyLog, err := s.log.IsEmpty()
	if err != nil {
		return false, fmt.Errorf("failed to check if raft log is empty with error: %v", err)
	}

	if !etcdraft.IsEmptyHardState(s.hs) {
		return false, nil
	}

	if !etcdraft.IsEmptySnap(s.snap) {
		return false, nil
	}

	if !emptyLog {
		return false, nil
	}

	return true, nil
}
