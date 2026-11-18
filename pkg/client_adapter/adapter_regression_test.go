package client_adapter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/local"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func newLocalTestRepo(t *testing.T, password string) *Repository {
	t.Helper()
	be, err := local.Create(context.Background(), local.Config{Path: t.TempDir(), Connections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := InitRepository(context.Background(), be, password)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// saveTestTree stores data blobs and a tree whose nodes are built by mkNodes.
func saveTestTree(t *testing.T, repo *Repository, mkNodes func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error)) ID {
	t.Helper()
	var treeID ID
	err := repo.WithBlobUploader(context.Background(), func(ctx context.Context, up restic.BlobSaverWithAsync) error {
		nodes, err := mkNodes(ctx, up)
		if err != nil {
			return err
		}
		buf, err := json.Marshal(map[string]any{"nodes": nodes})
		if err != nil {
			return err
		}
		treeID, _, _, err = up.SaveBlob(ctx, restic.TreeBlob, buf, restic.ID{}, false)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return treeID
}

func fileNode(ctx context.Context, up restic.BlobSaver, name string, contents ...string) (*data.Node, error) {
	node := &data.Node{Name: name, Type: data.NodeTypeFile, Mode: 0o644}
	for _, c := range contents {
		id, _, _, err := up.SaveBlob(ctx, restic.DataBlob, []byte(c), restic.ID{}, false)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, id)
		node.Size += uint64(len(c))
	}
	return node, nil
}

func saveTestSnapshot(t *testing.T, repo *Repository, tree ID) ID {
	t.Helper()
	id, err := SaveSnapshot(context.Background(), repo, &data.Snapshot{
		Time:     time.Now(),
		Tree:     &tree,
		Paths:    []string{"/source"},
		Hostname: "test-host",
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func countFiles(t *testing.T, repo *Repository, ft restic.FileType) int {
	t.Helper()
	n := 0
	if err := repo.List(context.Background(), ft, func(restic.ID, int64) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCopySnapshotsIncrementalSkipsKnownBlobs(t *testing.T) {
	ctx := context.Background()
	src := newLocalTestRepo(t, "src")
	dst := newLocalTestRepo(t, "dst")

	first := saveTestSnapshot(t, src, saveTestTree(t, src, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		n, err := fileNode(ctx, up, "a.txt", "shared")
		return []*data.Node{n}, err
	}))
	if _, stats, err := CopySnapshots(ctx, src, dst, []ID{first}); err != nil {
		t.Fatal(err)
	} else if stats.DataBlobs != 1 || stats.Skipped != 0 || stats.PacksCopied != stats.PacksTotal || stats.PacksTotal == 0 {
		t.Fatalf("first copy stats = %+v", stats)
	}

	// The second snapshot reuses "shared" and adds one new blob.
	second := saveTestSnapshot(t, src, saveTestTree(t, src, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		a, err := fileNode(ctx, up, "a.txt", "shared")
		if err != nil {
			return nil, err
		}
		b, err := fileNode(ctx, up, "b.txt", "new")
		return []*data.Node{a, b}, err
	}))

	var calls int
	copied, stats, err := CopySnapshotsWithProgress(ctx, src, dst, []ID{second}, func(CopyStats) { calls++ })
	if err != nil {
		t.Fatal(err)
	}
	if len(copied) != 1 || stats.Snapshots != 1 {
		t.Fatalf("copied = %v, stats = %+v", copied, stats)
	}
	if stats.DataBlobs != 1 || stats.TreeBlobs != 1 || stats.Skipped != 1 {
		t.Fatalf("want 1 new data blob, 1 new tree, 1 skipped; stats = %+v", stats)
	}
	if calls == 0 {
		t.Fatal("progress callback was never called")
	}

	sn, err := LoadSnapshot(ctx, dst, mustParseID(t, copied[0].DestinationID))
	if err != nil {
		t.Fatal(err)
	}
	if sn.Original == nil || *sn.Original != second {
		t.Fatalf("original = %v, want %v", sn.Original, second)
	}
	if err := Check(ctx, dst); err != nil {
		t.Fatalf("destination check: %v", err)
	}
}

func TestCopySnapshotsMissingSourceBlobFailsWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	src := newLocalTestRepo(t, "src")
	dst := newLocalTestRepo(t, "dst")

	missing := restic.Hash([]byte("never stored"))
	snap := saveTestSnapshot(t, src, saveTestTree(t, src, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		return []*data.Node{{Name: "gone.txt", Type: data.NodeTypeFile, Mode: 0o644, Content: restic.IDs{missing}}}, nil
	}))

	_, _, err := CopySnapshots(ctx, src, dst, []ID{snap})
	if err == nil || !strings.Contains(err.Error(), "missing from the source") {
		t.Fatalf("err = %v, want missing-blob error", err)
	}
	if n := countFiles(t, dst, restic.SnapshotFile); n != 0 {
		t.Fatalf("destination has %d snapshots, want 0", n)
	}
}

func TestRepairSnapshotsDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	repo := newLocalTestRepo(t, "pw")
	missing := restic.Hash([]byte("never stored"))
	snap := saveTestSnapshot(t, repo, saveTestTree(t, repo, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		ok, err := fileNode(ctx, up, "ok.txt", "present")
		if err != nil {
			return nil, err
		}
		broken := &data.Node{Name: "broken.txt", Type: data.NodeTypeFile, Mode: 0o644, Content: restic.IDs{missing}}
		return []*data.Node{broken, ok}, nil
	}))

	packs, indexes := countFiles(t, repo, restic.PackFile), countFiles(t, repo, restic.IndexFile)
	results, err := RepairSnapshots(ctx, repo, []ID{snap}, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].Changed || results[0].NewID != "" {
		t.Fatalf("results = %+v, want one changed snapshot without a new ID", results)
	}
	if p, i := countFiles(t, repo, restic.PackFile), countFiles(t, repo, restic.IndexFile); p != packs || i != indexes {
		t.Fatalf("dry run wrote data: packs %d->%d, indexes %d->%d", packs, p, indexes, i)
	}
	if n := countFiles(t, repo, restic.SnapshotFile); n != 1 {
		t.Fatalf("snapshots = %d, want 1", n)
	}

	results, err = RepairSnapshots(ctx, repo, []ID{snap}, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NewID == "" {
		t.Fatalf("results = %+v, want a repaired snapshot", results)
	}
}

func TestComputeDiffReportsUnreadableSubtree(t *testing.T) {
	ctx := context.Background()
	repo := newLocalTestRepo(t, "pw")
	missingTree := restic.Hash([]byte("no such tree"))

	before := saveTestTree(t, repo, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		n, err := fileNode(ctx, up, "a.txt", "a")
		return []*data.Node{n}, err
	})
	after := saveTestTree(t, repo, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		n, err := fileNode(ctx, up, "a.txt", "a")
		dir := &data.Node{Name: "dir", Type: data.NodeTypeDir, Mode: 0o755, Subtree: &missingTree}
		return []*data.Node{n, dir}, err
	})

	_, _, err := ComputeDiff(ctx, repo, &data.Snapshot{Tree: &before}, &data.Snapshot{Tree: &after})
	if err == nil || !strings.Contains(err.Error(), "/dir/") {
		t.Fatalf("err = %v, want error naming /dir/", err)
	}
}

func mustParseID(t *testing.T, s string) ID {
	t.Helper()
	id, err := restic.ParseID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// Syncter passes a nil *progress.Counter; restic calls Add unconditionally,
// so the adapter must not forward a typed-nil counter.
func TestNilProgressCounterDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	repo := newLocalTestRepo(t, "pw")
	tree := saveTestTree(t, repo, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		n, err := fileNode(ctx, up, "a.txt", "a")
		return []*data.Node{n}, err
	})
	err := StreamTrees(ctx, repo, []ID{tree}, nil, func(ID) bool { return false },
		func(_ ID, err error, nodes TreeNodeIterator) error {
			for range nodes {
			}
			return err
		})
	if err != nil {
		t.Fatal(err)
	}
	blobs := restic.NewBlobSet()
	if err := FindUsedBlobs(ctx, repo, []ID{tree}, blobs, nil); err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 2 {
		t.Fatalf("used blobs = %d, want tree + data", len(blobs))
	}
	saveTestSnapshot(t, repo, tree)
	used := restic.NewBlobSet()
	if err := CollectUsedBlobs(ctx, repo, used); err != nil {
		t.Fatal(err)
	}
	if len(used) != 2 {
		t.Fatalf("CollectUsedBlobs = %d blobs, want 2", len(used))
	}
}

// Prune/forget take an exclusive lock and must fail fast while a backup
// (non-exclusive lock) is running, instead of deleting packs underneath it.
func TestLockRepositoryExclusiveConflictsWithShared(t *testing.T) {
	ctx := context.Background()
	repo := newLocalTestRepo(t, "pw")

	unlockShared, _, err := LockRepository(ctx, repo, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := LockRepository(ctx, repo, true); err == nil || !IsAlreadyLocked(err) {
		t.Fatalf("exclusive lock while shared lock held: err = %v, want already-locked", err)
	}
	unlockShared()

	unlockExclusive, _, err := LockRepository(ctx, repo, true)
	if err != nil {
		t.Fatalf("exclusive lock after release: %v", err)
	}
	unlockExclusive()
}

// flakyBackend fails the first Save and Load calls with a transient error.
type flakyBackend struct {
	Backend
	saveFailures, loadFailures int
}

func (f *flakyBackend) Save(ctx context.Context, h Handle, rd RewindReader) error {
	if f.saveFailures > 0 {
		f.saveFailures--
		return errors.New("transient network error")
	}
	return f.Backend.Save(ctx, h, rd)
}

func (f *flakyBackend) Load(ctx context.Context, h Handle, length int, offset int64, fn func(io.Reader) error) error {
	if f.loadFailures > 0 && h.Type == backend.ConfigFile {
		f.loadFailures--
		return errors.New("transient network error")
	}
	return f.Backend.Load(ctx, h, length, offset, fn)
}

func TestWrapBackendRetriesTransientErrors(t *testing.T) {
	ctx := context.Background()
	be, err := local.Create(ctx, local.Config{Path: t.TempDir(), Connections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyBackend{Backend: be, saveFailures: 2}
	var retries int
	wrapped := WrapBackend(flaky, 30*time.Second, func(string, error, time.Duration) { retries++ })
	if _, err := InitRepository(ctx, wrapped, "pw"); err != nil {
		t.Fatalf("init through flaky backend: %v", err)
	}
	if retries != 2 {
		t.Fatalf("retries = %d, want 2", retries)
	}

	// Not-found must not be retried until the window expires.
	start := time.Now()
	err = wrapped.Load(ctx, Handle{Type: backend.SnapshotFile, Name: strings.Repeat("0", 64)}, 0, 0, func(io.Reader) error { return nil })
	if err == nil || !be.IsNotExist(err) {
		t.Fatalf("load missing file: err = %v, want not-exist", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("not-found was retried for %v", time.Since(start))
	}
}

// countingBackend counts index file loads that reach the backend.
type countingBackend struct {
	Backend
	indexLoads int
}

func (c *countingBackend) Load(ctx context.Context, h Handle, length int, offset int64, fn func(io.Reader) error) error {
	if h.Type == backend.IndexFile {
		c.indexLoads++
	}
	return c.Backend.Load(ctx, h, length, offset, fn)
}

func TestUseRepositoryCacheServesIndexFromDisk(t *testing.T) {
	ctx := context.Background()
	repoDir, cacheDir := t.TempDir(), t.TempDir()
	be, err := local.Create(ctx, local.Config{Path: repoDir, Connections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := InitRepository(ctx, be, "pw")
	if err != nil {
		t.Fatal(err)
	}
	saveTestSnapshot(t, repo, saveTestTree(t, repo, func(ctx context.Context, up restic.BlobSaver) ([]*data.Node, error) {
		n, err := fileNode(ctx, up, "a.txt", "a")
		return []*data.Node{n}, err
	}))

	open := func() *countingBackend {
		counting := &countingBackend{Backend: be}
		r, err := NewRepository(counting, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.SearchKey(ctx, "pw", 1, ""); err != nil {
			t.Fatal(err)
		}
		if err := UseRepositoryCache(r, cacheDir, nil); err != nil {
			t.Fatal(err)
		}
		if err := r.LoadIndex(ctx, nil); err != nil {
			t.Fatal(err)
		}
		return counting
	}
	if first := open(); first.indexLoads == 0 {
		t.Fatal("first open did not load the index from the backend")
	}
	if second := open(); second.indexLoads != 0 {
		t.Fatalf("second open loaded %d index files from the backend, want 0 (cached)", second.indexLoads)
	}
}
