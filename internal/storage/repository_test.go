package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rottentail/solo-0026-audiovisual-sync-conformer/internal/domain"
)

func testTake(id, name string) domain.MediaTake {
	return domain.MediaTake{
		ID:         id,
		Name:       name,
		Note:       "workflow calibration",
		State:      domain.TakeStateAdmitted,
		Audio:      domain.AudioInfo{Digest: "audio-" + id, ByteCount: 4},
		Video:      domain.VideoInfo{Digest: "video-" + id, ByteCount: 4},
		Digest:     "combined-" + id,
		AdmittedAt: time.Now().UTC(),
	}
}

// TestRepositoryRetiresIdentityLocks verifies that many completed admissions,
// analyses, and conforms leave no idle lock objects behind.
func TestRepositoryRetiresIdentityLocks(t *testing.T) {
	repository, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("take-%03d", i)
		record := testTake(id, "camera "+id)
		if _, created, err := repository.AdmitTake(record, []byte("aud"), []byte("vid")); err != nil || !created {
			t.Fatalf("admit %s: created=%v err=%v", id, created, err)
		}
		analysis := domain.SyncAnalysis{
			ID:        fmt.Sprintf("sync-%03d", i),
			TakeID:    id,
			State:     domain.AnalysisStateQueued,
			Policy:    domain.SyncPolicy{MinConfidence: 0.55, MaxOffsetMS: 1000},
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}
		if _, created, err := repository.CreateAnalysis(analysis); err != nil || !created {
			t.Fatalf("create analysis %s: created=%v err=%v", analysis.ID, created, err)
		}
		analysis.State = domain.AnalysisStateDetecting
		if err := repository.UpdateAnalysis(analysis); err != nil {
			t.Fatalf("advance analysis %s: %v", analysis.ID, err)
		}
	}
	if retained := repository.locks.count(); retained != 0 {
		t.Fatalf("%d idle identity locks were retained after 100 workflows", retained)
	}
}

// TestRepositoryConflictPreserved verifies the retry and conflict outcomes are
// unchanged by the lock lifecycle rework.
func TestRepositoryConflictPreserved(t *testing.T) {
	repository, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	record := testTake("stable-take-01", "stable take")
	if _, created, err := repository.AdmitTake(record, []byte("aud"), []byte("vid")); err != nil || !created {
		t.Fatalf("first admission: created=%v err=%v", created, err)
	}
	if _, created, err := repository.AdmitTake(record, []byte("aud"), []byte("vid")); err != nil || created {
		t.Fatalf("identical retry: created=%v err=%v, want existing record", created, err)
	}
	conflicting := record
	conflicting.Name = "other take"
	if _, _, err := repository.AdmitTake(conflicting, []byte("aud"), []byte("vid")); !domain.IsErrorCode(err, "take_conflict") {
		t.Fatalf("conflicting reuse returned %v, want take_conflict", err)
	}
	if retained := repository.locks.count(); retained != 0 {
		t.Fatalf("%d idle identity locks were retained", retained)
	}
}

// TestRepositorySameIdentityIsExclusiveUnderContention verifies that concurrent
// callers for one identity serialize: exactly one admission wins and the rest
// observe the admitted record or the stable conflict, never a split lock.
func TestRepositorySameIdentityIsExclusiveUnderContention(t *testing.T) {
	repository, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	const workers = 16
	var createdTotal int32
	var wait sync.WaitGroup
	failures := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			record := testTake("race-take-01", fmt.Sprintf("camera %d", index))
			_, created, err := repository.AdmitTake(record, []byte("aud"), []byte("vid"))
			if err != nil {
				if !domain.IsErrorCode(err, "take_conflict") {
					failures <- err
				}
				return
			}
			if created {
				atomic.AddInt32(&createdTotal, 1)
			}
		}(i)
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Fatalf("unexpected admission error: %v", err)
	}
	if got := atomic.LoadInt32(&createdTotal); got != 1 {
		t.Fatalf("%d admissions reported creation, want exactly one", got)
	}
	if retained := repository.locks.count(); retained != 0 {
		t.Fatalf("%d idle identity locks were retained", retained)
	}
}
