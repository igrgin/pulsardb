package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/tidwall/wal"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	RecordTypeEntry     byte = 1
	RecordTypeHardState byte = 2
	RecordTypeSnapshot  byte = 3
	RecordTypeConfState byte = 4
)

const (
	snapshotFolder = "snapshot"
	walFolder      = "wal"
)

type Storage struct {
	mu sync.Mutex

	dir string
	log *wal.Log
	ms  *etcdraft.MemoryStorage

	hs        raftpb.HardState
	snap      raftpb.Snapshot
	confState raftpb.ConfState

	nextWALIdx uint64
	entryIndex map[uint64]uint64
}

func OpenStorage(dir string, noSync bool) (*Storage, uint64, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	snapDataDir := filepath.Join(dir, snapshotFolder)
	if err := os.MkdirAll(snapDataDir, 0o750); err != nil {
		return nil, 0, fmt.Errorf("mkdir snapdata: %w", err)
	}

	opts := *wal.DefaultOptions
	opts.NoSync = noSync
	log, err := wal.Open(filepath.Join(dir, walFolder), &opts)
	if err != nil {
		return nil, 0, fmt.Errorf("wal.Open: %w", err)
	}

	s := &Storage{
		dir:        dir,
		log:        log,
		ms:         etcdraft.NewMemoryStorage(),
		entryIndex: make(map[uint64]uint64),
		nextWALIdx: 1,
	}

	applied, err := s.replay()
	if err != nil {
		log.Close()
		return nil, 0, err
	}

	return s, applied, nil
}

func (s *Storage) replay() (uint64, error) {
	empty, err := s.log.IsEmpty()
	if err != nil {
		return 0, fmt.Errorf("wal.IsEmpty: %w", err)
	}
	if empty {
		return 0, nil
	}

	first, err := s.log.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("wal.FirstIndex: %w", err)
	}
	last, err := s.log.LastIndex()
	if err != nil {
		return 0, fmt.Errorf("wal.LastIndex: %w", err)
	}

	var allEntries []raftpb.Entry
	var lastValidSnapMeta *raftpb.SnapshotMetadata
	var lastValidSnapData []byte

	for idx := first; idx <= last; idx++ {
		data, err := s.log.Read(idx)
		if err != nil {
			return 0, fmt.Errorf("wal.Read(%d): %w", idx, err)
		}

		recType, payload, err := unmarshalRecord(data)
		if err != nil {
			return 0, fmt.Errorf("unmarshal record %d: %w", idx, err)
		}

		switch recType {
		case RecordTypeEntry:
			var e raftpb.Entry
			pbutil.MustUnmarshal(&e, payload)
			s.entryIndex[e.Index] = idx
			allEntries = append(allEntries, e)

		case RecordTypeHardState:
			s.hs = raftpb.HardState{}
			pbutil.MustUnmarshal(&s.hs, payload)

		case RecordTypeConfState:
			s.confState = raftpb.ConfState{}
			pbutil.MustUnmarshal(&s.confState, payload)
			slog.Debug("restored confState from WAL",
				"voters", s.confState.Voters,
				"learners", s.confState.Learners,
			)

		case RecordTypeSnapshot:
			var snapMeta raftpb.SnapshotMetadata
			pbutil.MustUnmarshal(&snapMeta, payload)

			if data, err := s.loadSnapshotData(snapMeta.Index); err == nil {
				lastValidSnapMeta = &raftpb.SnapshotMetadata{}
				*lastValidSnapMeta = snapMeta
				lastValidSnapData = data
				s.confState = snapMeta.ConfState
				slog.Debug("found valid snapshot", "index", snapMeta.Index)
			} else {
				slog.Warn("snapshot data file missing, probably compacted, skipping",
					"index", snapMeta.Index,
					"error", err,
				)
			}
		}

		s.nextWALIdx = idx + 1
	}

	var snapIndex uint64
	if lastValidSnapMeta != nil {
		s.snap.Metadata = *lastValidSnapMeta
		s.snap.Data = lastValidSnapData
		snapIndex = lastValidSnapMeta.Index

		for ri := range s.entryIndex {
			if ri <= snapIndex {
				delete(s.entryIndex, ri)
			}
		}
	}

	var entries []raftpb.Entry
	for _, e := range allEntries {
		if e.Index > snapIndex {
			entries = append(entries, e)
		}
	}

	if !etcdraft.IsEmptySnap(s.snap) {
		if err := s.ms.ApplySnapshot(s.snap); err != nil &&
			!errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			return 0, fmt.Errorf("apply snapshot: %w", err)
		}
	} else if len(s.confState.Voters) > 0 {
		dummySnap := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index:     s.hs.Commit,
				Term:      s.hs.Term,
				ConfState: s.confState,
			},
		}
		if err := s.ms.ApplySnapshot(dummySnap); err != nil &&
			!errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			return 0, fmt.Errorf("apply confState snapshot: %w", err)
		}
		slog.Debug("applied confState to MemoryStorage via dummy snapshot",
			"voters", s.confState.Voters,
			"commit", s.hs.Commit,
		)
	}

	if !etcdraft.IsEmptyHardState(s.hs) {
		if err := s.ms.SetHardState(s.hs); err != nil {
			return 0, fmt.Errorf("set hardstate: %w", err)
		}
	}

	if len(entries) > 0 {
		if err := s.ms.Append(entries); err != nil {
			return 0, fmt.Errorf("append entries: %w", err)
		}
	}

	applied := s.snap.Metadata.Index
	if s.hs.Commit > applied {
		applied = s.hs.Commit
	}

	slog.Info("replayed WAL",
		"wal_first", first,
		"wal_last", last,
		"entries", len(entries),
		"snap_index", snapIndex,
		"hs_commit", s.hs.Commit,
		"applied", applied,
		"voters", s.confState.Voters,
	)

	return applied, nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		return s.log.Close()
	}
	return nil
}

func (s *Storage) SaveReady(rd etcdraft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := s.applyReceivedSnapshotLocked(rd.Snapshot); err != nil {
			return err
		}
	}

	for i := range rd.Entries {
		if err := s.appendRecordLocked(RecordTypeEntry, &rd.Entries[i]); err != nil {
			return err
		}
		s.entryIndex[rd.Entries[i].Index] = s.nextWALIdx - 1
	}
	if len(rd.Entries) > 0 {
		if err := s.ms.Append(rd.Entries); err != nil {
			return fmt.Errorf("MemoryStorage.Append: %w", err)
		}
	}

	hsChanged := !etcdraft.IsEmptyHardState(rd.HardState) &&
		!isHardStateEqual(s.hs, rd.HardState)
	if hsChanged {
		if err := s.appendRecordLocked(RecordTypeHardState, &rd.HardState); err != nil {
			return err
		}
		s.hs = rd.HardState
		if err := s.ms.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("MemoryStorage.SetHardState: %w", err)
		}
	}

	if rd.MustSync {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("wal.Sync: %w", err)
		}
	}

	return nil
}

func (s *Storage) applyReceivedSnapshotLocked(snap raftpb.Snapshot) error {
	if len(snap.Data) > 0 {
		if err := s.saveSnapshotData(snap); err != nil {
			return fmt.Errorf("save snapshot data: %w", err)
		}
	}

	if err := s.appendRecordLocked(RecordTypeSnapshot, &snap.Metadata); err != nil {
		return fmt.Errorf("append snapshot record: %w", err)
	}

	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("wal.Sync: %w", err)
	}

	if err := s.ms.ApplySnapshot(snap); err != nil &&
		!errors.Is(err, etcdraft.ErrSnapOutOfDate) {
		return fmt.Errorf("ApplySnapshot: %w", err)
	}

	s.snap = snap
	s.confState = snap.Metadata.ConfState

	for ri := range s.entryIndex {
		if ri <= snap.Metadata.Index {
			delete(s.entryIndex, ri)
		}
	}

	slog.Info("applied snapshot from leader",
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
	)

	return nil
}

func (s *Storage) SaveConfState(cs raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.appendRecordLocked(RecordTypeConfState, &cs); err != nil {
		return fmt.Errorf("append confState record: %w", err)
	}

	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("wal.Sync: %w", err)
	}

	s.confState = cs

	slog.Debug("saved confState",
		"voters", cs.Voters,
		"learners", cs.Learners,
	)

	return nil
}

func (s *Storage) CreateSnapshot(index uint64, confState *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.ms.CreateSnapshot(index, confState, data)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	return snap, nil
}

func (s *Storage) SaveSnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(snap.Data) > 0 {
		if err := s.saveSnapshotData(snap); err != nil {
			return fmt.Errorf("save snapshot data: %w", err)
		}
	}

	if err := s.appendRecordLocked(RecordTypeSnapshot, &snap.Metadata); err != nil {
		return fmt.Errorf("append snapshot record: %w", err)
	}

	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("wal.Sync: %w", err)
	}

	s.snap = snap
	s.confState = snap.Metadata.ConfState

	for ri := range s.entryIndex {
		if ri <= snap.Metadata.Index {
			delete(s.entryIndex, ri)
		}
	}

	slog.Info("saved snapshot",
		"index", snap.Metadata.Index,
		"term", snap.Metadata.Term,
	)

	return nil
}

func (s *Storage) Compact(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ms.Compact(compactIndex); err != nil {
		if !errors.Is(err, etcdraft.ErrCompacted) {
			return fmt.Errorf("MemoryStorage.Compact: %w", err)
		}
	}

	walIdx := s.findWALIndexForCompaction(compactIndex)
	if walIdx > 0 {
		if err := s.log.TruncateFront(walIdx); err != nil {
			return fmt.Errorf("wal.TruncateFront: %w", err)
		}

		for ri, wi := range s.entryIndex {
			if wi <= walIdx {
				delete(s.entryIndex, ri)
			}
		}
	}

	s.cleanupOldSnapshots(compactIndex)

	return nil
}

func (s *Storage) findWALIndexForCompaction(compactIndex uint64) uint64 {
	if walIdx, ok := s.entryIndex[compactIndex]; ok {
		return walIdx
	}

	var bestWalIdx uint64
	for ri, wi := range s.entryIndex {
		if ri <= compactIndex && wi > bestWalIdx {
			bestWalIdx = wi
		}
	}

	return bestWalIdx
}

func (s *Storage) saveSnapshotData(snap raftpb.Snapshot) error {
	path := filepath.Join(s.dir, snapshotFolder, fmt.Sprintf("%016x", snap.Metadata.Index))

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(snap.Data); err != nil {
		return err
	}

	return f.Sync()
}

func (s *Storage) loadSnapshotData(index uint64) ([]byte, error) {
	path := filepath.Join(s.dir, snapshotFolder, fmt.Sprintf("%016x", index))
	return os.ReadFile(path)
}

func (s *Storage) cleanupOldSnapshots(keepAfterIndex uint64) {
	snapDir := filepath.Join(s.dir, snapshotFolder)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return
	}

	currentSnapIndex := s.snap.Metadata.Index

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(e.Name(), "%016x", &idx); err != nil {
			continue
		}
		if idx < keepAfterIndex && idx != currentSnapIndex {
			path := filepath.Join(snapDir, e.Name())
			if err := os.Remove(path); err != nil {
				slog.Warn("failed to remove old snapshot", "path", path, "error", err)
			}
		}
	}
}

func (s *Storage) appendRecordLocked(recType byte, msg interface{ Marshal() ([]byte, error) }) error {
	payload, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}

	slog.Debug("Appending record to log", "index", s.nextWALIdx)
	data := marshalRecord(recType, payload)
	if err := s.log.Write(s.nextWALIdx, data); err != nil {
		return fmt.Errorf("wal.Write(%d): %w", s.nextWALIdx, err)
	}
	s.nextWALIdx++
	return nil
}

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

	commitIndex := s.hs.Commit
	if commitIndex == 0 {
		return nil, nil
	}

	var entries []raftpb.Entry
	for idx := first; idx <= last; idx++ {
		data, err := s.log.Read(idx)
		if err != nil {
			return nil, fmt.Errorf("wal.Read(%d): %w", idx, err)
		}

		recType, payload, err := unmarshalRecord(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal record %d: %w", idx, err)
		}

		if recType != RecordTypeEntry {
			continue
		}

		var e raftpb.Entry
		pbutil.MustUnmarshal(&e, payload)

		if e.Index <= afterIndex {
			continue
		}
		if e.Index > commitIndex {
			break
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func (s *Storage) RaftStorage() *etcdraft.MemoryStorage {
	return s.ms
}

func (s *Storage) SnapshotIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap.Metadata.Index
}

func (s *Storage) SnapshotData() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap.Data
}

func (s *Storage) ConfState() raftpb.ConfState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.confState
}

func (s *Storage) HardState() raftpb.HardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs
}

func (s *Storage) IsStorageEmpty() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	empty, err := s.log.IsEmpty()
	if err != nil {
		return false, fmt.Errorf("wal.IsEmpty: %w", err)
	}

	if !etcdraft.IsEmptyHardState(s.hs) {
		return false, nil
	}
	if s.snap.Metadata.Index != 0 {
		return false, nil
	}
	if len(s.confState.Voters) > 0 {
		return false, nil
	}

	return empty, nil
}

func marshalRecord(recType byte, payload []byte) []byte {
	buf := make([]byte, 1+binary.MaxVarintLen64+len(payload))
	buf[0] = recType
	n := binary.PutUvarint(buf[1:], uint64(len(payload)))
	copy(buf[1+n:], payload)
	return buf[:1+n+len(payload)]
}

func unmarshalRecord(data []byte) (byte, []byte, error) {
	if len(data) < 2 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	recType := data[0]
	length, n := binary.Uvarint(data[1:])
	if n <= 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	start := 1 + n
	end := start + int(length)
	if end > len(data) {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return recType, data[start:end], nil
}

func isHardStateEqual(a, b raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}
