package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/tidwall/wal"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	hardStateFolder = "hard_state"
	snapshotFolder  = "snapshot"
	walFolder       = "wal"
	confStateFolder = "conf_state"
)

type Storage struct {
	mu sync.Mutex

	dir string
	log *wal.Log
	ms  *etcdraft.MemoryStorage

	hs        raftpb.HardState
	snap      raftpb.Snapshot
	confState raftpb.ConfState
}

// OpenStorage opens or creates a Storage in the given directory.
func OpenStorage(dir string) (*Storage, uint64, error) {
	format := "mkdir %s:  %w"
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, fmt.Errorf(format, dir, err)
	}

	for _, folder := range []string{walFolder, snapshotFolder, hardStateFolder, confStateFolder} {
		if err := os.MkdirAll(path.Join(dir, folder), 0o750); err != nil {
			return nil, 0, fmt.Errorf(format, path.Join(dir, folder), err)
		}
	}

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

	// Load in correct order:  snapshot first, then hardstate, then confstate, then WAL
	if err := s.loadSnapshot(); err != nil {
		s.log.Close()
		return nil, 0, err
	}
	if err := s.loadHardState(); err != nil {
		s.log.Close()
		return nil, 0, err
	}
	if err := s.loadConfState(); err != nil {
		s.log.Close()
		return nil, 0, err
	}
	if err := s.replayWAL(); err != nil {
		s.log.Close()
		return nil, 0, err
	}

	// CRITICAL: Apply confState to MemoryStorage so raft knows about voters!
	if err := s.applyConfStateToMemoryStorage(); err != nil {
		s.log.Close()
		return nil, 0, err
	}

	var lastIndex uint64
	if last, err := s.ms.LastIndex(); err == nil {
		lastIndex = last
	}

	return s, lastIndex, nil
}

// applyConfStateToMemoryStorage creates a dummy snapshot with confState
// so that raft's MemoryStorage knows about the cluster configuration.
func (s *Storage) applyConfStateToMemoryStorage() error {
	if len(s.confState.Voters) == 0 {
		return nil // No confState to apply
	}

	// Get the current snapshot from memory storage
	existingSnap, err := s.ms.Snapshot()
	if err != nil {
		return fmt.Errorf("get existing snapshot: %w", err)
	}

	// If existing snapshot already has confState, we're good
	if len(existingSnap.Metadata.ConfState.Voters) > 0 {
		slog.Debug("MemoryStorage already has confState",
			"voters", existingSnap.Metadata.ConfState.Voters,
		)
		return nil
	}

	// We need to create a snapshot with the confState
	// Use the last applied index from hardstate or existing snapshot
	snapIndex := existingSnap.Metadata.Index
	snapTerm := existingSnap.Metadata.Term

	if s.hs.Commit > snapIndex {
		snapIndex = s.hs.Commit
		snapTerm = s.hs.Term
	}

	// If we have no index, use index 0 (initial state)
	// The confState still needs to be set
	newSnap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapIndex,
			Term:      snapTerm,
			ConfState: s.confState,
		},
		Data: existingSnap.Data, // Preserve any existing snapshot data
	}

	if err := s.ms.ApplySnapshot(newSnap); err != nil && !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
		return fmt.Errorf("apply confState snapshot: %w", err)
	}

	// Re-apply hardstate after snapshot
	if !etcdraft.IsEmptyHardState(s.hs) {
		if err := s.ms.SetHardState(s.hs); err != nil {
			return fmt.Errorf("re-set hardstate: %w", err)
		}
	}

	// Re-replay WAL entries after snapshot (they may have been cleared)
	if err := s.replayWAL(); err != nil {
		return fmt.Errorf("re-replay WAL: %w", err)
	}

	slog.Info("applied confState to MemoryStorage",
		"voters", s.confState.Voters,
		"learners", s.confState.Learners,
		"snap_index", snapIndex,
		"snap_term", snapTerm,
	)

	return nil
}

// ConfState returns the current cluster membership configuration.
func (s *Storage) ConfState() raftpb.ConfState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.confState
}

// RaftStorage returns the etcdraft.Storage implementation.
func (s *Storage) RaftStorage() *etcdraft.MemoryStorage {
	return s.ms
}

// Close closes the WAL and releases resources.
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

	fileName, err := s.findSingleFile(dir)
	if err != nil {
		return err
	}
	if fileName == "" {
		return nil
	}

	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("read snapshot:  %w", err)
	}

	pbutil.MustUnmarshal(&s.snap, data)

	if err := s.ms.ApplySnapshot(s.snap); err != nil && !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
		return fmt.Errorf("ApplySnapshot: %w", err)
	}

	// Also restore confState from snapshot if present
	if len(s.snap.Metadata.ConfState.Voters) > 0 || len(s.snap.Metadata.ConfState.Learners) > 0 {
		s.confState = s.snap.Metadata.ConfState
	}

	slog.Debug("loaded snapshot",
		"index", s.snap.Metadata.Index,
		"term", s.snap.Metadata.Term,
		"voters", s.snap.Metadata.ConfState.Voters,
	)

	return nil
}

func (s *Storage) loadHardState() error {
	dir := filepath.Join(s.dir, hardStateFolder)

	fileName, err := s.findSingleFile(dir)
	if err != nil {
		return err
	}
	if fileName == "" {
		return nil
	}

	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("read hardstate: %w", err)
	}

	pbutil.MustUnmarshal(&s.hs, data)
	if err := s.ms.SetHardState(s.hs); err != nil {
		return fmt.Errorf("failed to set hardstate: %w", err)
	}

	slog.Debug("loaded hardstate",
		"term", s.hs.Term,
		"vote", s.hs.Vote,
		"commit", s.hs.Commit,
	)

	return nil
}

func (s *Storage) loadConfState() error {
	dir := filepath.Join(s.dir, confStateFolder)

	fileName, err := s.findSingleFile(dir)
	if err != nil {
		return err
	}
	if fileName == "" {
		return nil
	}

	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		return fmt.Errorf("read confstate: %w", err)
	}

	pbutil.MustUnmarshal(&s.confState, data)

	slog.Debug("loaded confState",
		"voters", s.confState.Voters,
		"learners", s.confState.Learners,
	)

	return nil
}

func (s *Storage) replayWAL() error {
	empty, err := s.log.IsEmpty()
	if err != nil {
		return fmt.Errorf("wal.IsEmpty: %w", err)
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
			return fmt.Errorf("wal.Read(%d): %w", idx, err)
		}
		var e raftpb.Entry
		pbutil.MustUnmarshal(&e, data)
		if e.Index <= snapIndex {
			continue
		}
		ents = append(ents, e)
	}

	if len(ents) > 0 {
		if err := s.ms.Append(ents); err != nil {
			return fmt.Errorf("MemoryStorage.Append: %w", err)
		}
		slog.Debug("replayed WAL to MemoryStorage",
			"count", len(ents),
			"first", ents[0].Index,
			"last", ents[len(ents)-1].Index,
		)
	}

	return nil
}

// EntriesAfter returns all committed entries after the given index.
func (s *Storage) EntriesAfter(afterIndex uint64) ([]raftpb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	empty, err := s.log.IsEmpty()
	if err != nil {
		return nil, fmt.Errorf("wal.IsEmpty: %w", err)
	}
	if empty {
		return nil, nil
	}

	first, err := s.log.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("wal.FirstIndex: %w", err)
	}
	last, err := s.log.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("wal.LastIndex: %w", err)
	}

	startIdx := afterIndex + 1
	if startIdx < first {
		startIdx = first
	}

	if startIdx > last {
		return nil, nil
	}

	commitIndex := s.hs.Commit
	if commitIndex == 0 || commitIndex < startIdx {
		return nil, nil
	}
	if last > commitIndex {
		last = commitIndex
	}

	var entries []raftpb.Entry
	for idx := startIdx; idx <= last; idx++ {
		data, err := s.log.Read(idx)
		if err != nil {
			return nil, fmt.Errorf("wal.Read(%d): %w", idx, err)
		}
		var e raftpb.Entry
		pbutil.MustUnmarshal(&e, data)
		entries = append(entries, e)
	}

	return entries, nil
}

// ---------- Persist Ready from etcdraft.Node ----------

func (s *Storage) SaveReady(rd etcdraft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) Snapshot
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := s.saveSnapshot(rd.Snapshot); err != nil {
			return err
		}
		if err := s.ms.ApplySnapshot(rd.Snapshot); err != nil && !errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			return fmt.Errorf("ApplySnapshot: %w", err)
		}
		s.confState = rd.Snapshot.Metadata.ConfState
	}

	// 2) Entries
	if len(rd.Entries) > 0 {
		if err := s.appendToWAL(rd.Entries); err != nil {
			return err
		}
		if err := s.ms.Append(rd.Entries); err != nil {
			return fmt.Errorf("MemoryStorage. Append: %w", err)
		}
	}

	// 3) HardState
	if !etcdraft.IsEmptyHardState(rd.HardState) {
		if !isHardStateEqual(s.hs, rd.HardState) {
			if err := s.saveHardState(rd.HardState); err != nil {
				return err
			}
			s.hs = rd.HardState
			if err := s.ms.SetHardState(rd.HardState); err != nil {
				return fmt.Errorf("failed to set hardstate: %w", err)
			}
		}
	}

	// 4) Sync if required
	if rd.MustSync {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("wal.Sync: %w", err)
		}
	}

	return nil
}

// SaveConfState persists the cluster membership.
func (s *Storage) SaveConfState(cs raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveConfState(cs)
}

// SaveSnapshot persists a snapshot to disk.
func (s *Storage) SaveSnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveSnapshot(snap)
}

func (s *Storage) saveConfState(cs raftpb.ConfState) error {
	confStatePath := filepath.Join(s.dir, confStateFolder, "current")
	data := pbutil.MustMarshal(&cs)
	if err := atomicWriteFile(confStatePath, data, 0o640); err != nil {
		return fmt.Errorf("write confstate: %w", err)
	}
	s.confState = cs
	return nil
}

func (s *Storage) appendToWAL(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	firstNew := ents[0].Index
	lastNew := ents[len(ents)-1].Index

	empty, err := s.log.IsEmpty()
	if err != nil {
		return fmt.Errorf("wal.IsEmpty: %w", err)
	}

	if !empty {
		last, err := s.log.LastIndex()
		if err != nil {
			return fmt.Errorf("wal.LastIndex: %w", err)
		}

		if last >= firstNew {
			if err := s.log.TruncateBack(firstNew - 1); err != nil {
				return fmt.Errorf("wal.TruncateBack(%d): %w", firstNew-1, err)
			}
			last = firstNew - 1
		}

		if last+1 != firstNew {
			return fmt.Errorf("wal append gap: last=%d, firstNew=%d", last, firstNew)
		}
	}

	for _, e := range ents {
		data := pbutil.MustMarshal(&e)
		if err := s.log.Write(e.Index, data); err != nil {
			return fmt.Errorf("wal.Write(%d): %w", e.Index, err)
		}
	}

	if last, err := s.log.LastIndex(); err == nil && last != lastNew {
		return fmt.Errorf("wal last index mismatch: got=%d want=%d", last, lastNew)
	}

	return nil
}

func (s *Storage) saveHardState(hs raftpb.HardState) error {
	hardStatePath := filepath.Join(s.dir, hardStateFolder, "current")
	data := pbutil.MustMarshal(&hs)
	if err := atomicWriteFile(hardStatePath, data, 0o640); err != nil {
		return fmt.Errorf("write hardstate:  %w", err)
	}
	return nil
}

func (s *Storage) saveSnapshot(snap raftpb.Snapshot) error {
	snapshotPath := filepath.Join(s.dir, snapshotFolder, "current")
	data := pbutil.MustMarshal(&snap)
	if err := atomicWriteFile(snapshotPath, data, 0o640); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	s.snap = snap
	return nil
}

// ---------- helpers ----------

func (s *Storage) findSingleFile(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("read dir %s: %w", dir, err)
	}
	var fileName string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if fileName != "" {
			return "", fmt.Errorf("multiple files in %s", dir)
		}
		fileName = e.Name()
	}
	return fileName, nil
}

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
		return false, fmt.Errorf("failed to check if raft log is empty: %w", err)
	}

	if !etcdraft.IsEmptyHardState(s.hs) {
		return false, nil
	}

	if !etcdraft.IsEmptySnap(s.snap) {
		return false, nil
	}

	if len(s.confState.Voters) > 0 {
		return false, nil
	}

	if !emptyLog {
		return false, nil
	}

	return true, nil
}
