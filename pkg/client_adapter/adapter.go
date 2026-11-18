package client_adapter

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/cache"
	"github.com/restic/restic/internal/backend/retry"
	"github.com/restic/restic/internal/backend/sema"
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/filter"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/repository/index"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
	"github.com/restic/restic/internal/ui"
	"github.com/restic/restic/internal/ui/progress"
	restoreui "github.com/restic/restic/internal/ui/restore"
	"github.com/restic/restic/internal/walker"
)

const (
	ResticExitSuccess                = 0
	ResticExitFailure                = 1
	ResticExitGoRuntimeError         = 2
	ResticExitIncompleteSourceData   = 3
	ResticExitRepositoryDoesNotExist = 10
	ResticExitRepositoryLocked       = 11
	ResticExitWrongPassword          = 12
	ResticExitInterrupted            = 130
)

// ============================================================================
// EXPORT TYPES
// ============================================================================

type Repository = repository.Repository
type RepositoryInterface = restic.Repository
type Backend = backend.Backend
type Snapshot = data.Snapshot
type Options = repository.Options
type ID = restic.ID
type FileType = backend.FileType
type Handle = backend.Handle
type FileInfo = backend.FileInfo
type RewindReader = backend.RewindReader
type BlobType = restic.BlobType
type BackendProperties = backend.Properties
type CompressionMode = repository.CompressionMode

// ============================================================================
// REPOSITORY
// ============================================================================

func NewRepository(be Backend, opts Options) (*Repository, error) {
	return repository.New(be, opts)
}

// UseRepositoryCache enables restic's local cache under cacheDir for an
// unlocked repository. Call it before LoadIndex: the cache keeps index and
// snapshot files plus the packs holding tree blobs on local disk, so later
// index loads and snapshot browsing do not download them again. Caches of
// repositories unused for 30 days are removed, as the restic CLI does.
func UseRepositoryCache(repo *Repository, cacheDir string, errorLog func(string, ...any)) error {
	if cacheDir == "" {
		return stderrors.New("cache directory is required")
	}
	if errorLog == nil {
		errorLog = func(string, ...any) {}
	}
	c, err := cache.New(repo.Config().ID, cacheDir)
	if err != nil {
		return err
	}
	repo.UseCache(c, errorLog)
	if old, err := cache.Old(c.Base); err == nil {
		for _, item := range old {
			_ = os.RemoveAll(filepath.Join(c.Base, item.Name()))
		}
	}
	return nil
}

// DefaultBackendRetryWindow bounds how long WrapBackend retries a failing
// request. The restic CLI uses 15 minutes; an app needs to surface errors to
// the user sooner, while still riding out network handovers and brief
// provider hiccups.
const DefaultBackendRetryWindow = 2 * time.Minute

// WrapBackend adds the layers the restic CLI puts around every backend before
// opening a repository: a connection limiter (at most
// be.Properties().Connections concurrent requests) and retries with
// exponential backoff for transient errors, for at most maxElapsed. Errors for
// which be.IsPermanentError (e.g. not found) returns true are not retried.
// report, when non-nil, is called before each retry.
func WrapBackend(be Backend, maxElapsed time.Duration, report func(msg string, err error, wait time.Duration)) Backend {
	return retry.New(sema.NewBackend(be), maxElapsed, report, nil)
}

func ParseCompressionMode(value string) (CompressionMode, error) {
	mode := repository.CompressionAuto
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return mode, nil
	}

	if err := mode.Set(trimmed); err != nil {
		return mode, err
	}

	return mode, nil
}

// ============================================================================
// FS
// ============================================================================

type FileSystem = fs.FS
type File = fs.File
type ExtendedFileInfo = fs.ExtendedFileInfo

type LocalFS = fs.FS

func NewLocalFS() LocalFS {
	return fs.NewLocal()
}

const (
	O_NOFOLLOW  = fs.O_NOFOLLOW
	O_DIRECTORY = fs.O_DIRECTORY
)

func Readdirnames(filesystem FileSystem, dir string, flags int) ([]string, error) {
	return fs.Readdirnames(filesystem, dir, flags)
}

// ============================================================================
// ARCHIVER
// ============================================================================

func NewArchiver(repo restic.Repository, fs fs.FS, opts archiver.Options) *archiver.Archiver {
	return archiver.New(repo, fs, opts)
}

type ArchiverOptions = archiver.Options
type SnapshotOptions = archiver.SnapshotOptions
type ItemStats = archiver.ItemStats
type ItemAction = archiver.ItemAction

const (
	ActionDirNew        = archiver.ActionDirNew
	ActionDirUnchanged  = archiver.ActionDirUnchanged
	ActionDirModified   = archiver.ActionDirModified
	ActionFileNew       = archiver.ActionFileNew
	ActionFileUnchanged = archiver.ActionFileUnchanged
	ActionFileModified  = archiver.ActionFileModified
)

type Scanner = archiver.Scanner
type ScanStats = archiver.ScanStats
type SelectByNameFunc = archiver.SelectByNameFunc
type SelectFunc = archiver.SelectFunc
type RejectByNameFunc = archiver.RejectByNameFunc
type RejectFunc = archiver.RejectFunc
type IncludeByNameFunc = filter.IncludeByNameFunc

func NewScanner(filesystem fs.FS) *Scanner {
	return archiver.NewScanner(filesystem)
}

func CombineRejectByNames(funcs []RejectByNameFunc) SelectByNameFunc {
	return archiver.CombineRejectByNames(funcs)
}

func CombineRejects(funcs []RejectFunc) SelectFunc {
	return archiver.CombineRejects(funcs)
}

func CombineIncludeByNames(funcs []IncludeByNameFunc) SelectByNameFunc {
	return func(item string) bool {
		if len(funcs) == 0 {
			return true
		}

		for _, include := range funcs {
			matched, childMayMatch := include(item)
			if matched || childMayMatch {
				return true
			}
		}

		return false
	}
}

func RejectByDevice(samples []string, filesystem fs.FS) (RejectFunc, error) {
	return archiver.RejectByDevice(samples, filesystem)
}

func RejectBySize(maxSize int64) (RejectFunc, error) {
	return archiver.RejectBySize(maxSize)
}

func RejectIfPresent(excludeFileSpec string, warnf func(msg string, args ...interface{})) (RejectFunc, error) {
	return archiver.RejectIfPresent(excludeFileSpec, warnf)
}

func IncludeByPattern(patterns []string, warnf func(msg string, args ...interface{})) IncludeByNameFunc {
	return filter.IncludeByPattern(patterns, warnf)
}

func IncludeByInsensitivePattern(patterns []string, warnf func(msg string, args ...interface{})) IncludeByNameFunc {
	return filter.IncludeByInsensitivePattern(patterns, warnf)
}

func RejectByPattern(patterns []string, warnf func(msg string, args ...interface{})) RejectByNameFunc {
	return archiver.RejectByNameFunc(filter.RejectByPattern(patterns, warnf))
}

func RejectByInsensitivePattern(patterns []string, warnf func(msg string, args ...interface{})) RejectByNameFunc {
	return archiver.RejectByNameFunc(filter.RejectByInsensitivePattern(patterns, warnf))
}

func ParseBytes(value string) (int64, error) {
	return ui.ParseBytes(value)
}

// ============================================================================
// RESTORER
// ============================================================================

type RestoreOptions = restorer.Options

func NewRestorer(repo restic.Repository, sn *Snapshot, opts RestoreOptions) *restorer.Restorer {
	return restorer.NewRestorer(repo, sn, opts)
}

func SetRepositoryDryRun(repo *Repository) {
	repo.SetDryRun()
}

// ============================================================================
// CHECKER
// ============================================================================

// NewChecker returns a checker for repo. repo must be a *Repository (as
// returned by NewRepository/InitRepository); checker.New needs the concrete
// type.
func NewChecker(repo restic.Repository, checkUnused bool) *checker.Checker {
	return checker.New(repo.(*repository.Repository), checkUnused)
}

// CheckerLoadSnapshots loads the snapshots named in args (all snapshots when
// args is empty) without any host/path/tag filtering.
func CheckerLoadSnapshots(ctx context.Context, c *checker.Checker, args []string) error {
	return c.LoadSnapshots(ctx, &data.SnapshotFilter{}, args)
}

func CheckerStructure(ctx context.Context, c *checker.Checker, p *progress.Counter, errChan chan<- error) {
	c.Structure(ctx, counterOrNoop(p), errChan)
}

// ============================================================================
// SNAPSHOT REPAIR
// ============================================================================

type RepairedSnapshot struct {
	SourceID string `json:"source_id"`
	NewID    string `json:"new_id,omitempty"`
	Changed  bool   `json:"changed"`
	Removed  bool   `json:"removed"`
}

// dryRunBlobSaver computes blob IDs without storing anything, so a dry-run
// tree rewrite yields the same tree IDs as a real one but writes no packs or
// index files. It is used instead of Repository.SetDryRun because callers
// cache and share *Repository values, and SetDryRun cannot be undone.
type dryRunBlobSaver struct {
	repo *Repository
}

func (s dryRunBlobSaver) SaveBlob(_ context.Context, t restic.BlobType, buf []byte, id restic.ID, _ bool) (restic.ID, bool, int, error) {
	if id.IsNull() {
		id = restic.Hash(buf)
	}
	_, known := s.repo.LookupBlobSize(restic.BlobHandle{ID: id, Type: t})
	return id, known, 0, nil
}

func (s dryRunBlobSaver) SaveBlobAsync(ctx context.Context, t restic.BlobType, buf []byte, id restic.ID, storeDuplicate bool, cb func(restic.ID, bool, int, error)) {
	cb(s.SaveBlob(ctx, t, buf, id, storeDuplicate))
}

// RepairSnapshots removes references to missing blobs and unreadable subtrees.
// With dryRun it reports what would change without writing to the repository.
// It deletes the damaged originals only when forget is true and dryRun is false.
func RepairSnapshots(ctx context.Context, repo *Repository, ids []ID, dryRun, forget bool) ([]RepairedSnapshot, error) {
	if err := repo.LoadIndex(ctx, nil); err != nil {
		return nil, err
	}
	rewriter := NewTreeRewriter(RewriteOpts{
		RewriteNode: func(node *Node, nodePath string) *Node {
			if node.Type == NodeTypeIrregular || node.Type == NodeTypeInvalid {
				return nil
			}
			if node.Type != NodeTypeFile {
				return node
			}
			newContent := restic.IDs{}
			var newSize uint64
			for _, id := range node.Content {
				size, found := repo.LookupBlobSize(restic.BlobHandle{Type: restic.DataBlob, ID: id})
				if found {
					newContent = append(newContent, id)
					newSize += uint64(size)
				}
			}
			node.Content = newContent
			node.Size = newSize
			return node
		},
		RewriteFailedTree: func(_ restic.ID, nodePath string, _ error) (data.TreeNodeIterator, error) {
			if nodePath == "/" {
				return nil, nil
			}
			return slices.Values([]data.NodeOrError{}), nil
		},
		AllowUnstableSerialization: true,
	})

	results := make([]RepairedSnapshot, 0, len(ids))
	for _, id := range ids {
		sn, err := LoadSnapshot(ctx, repo, id)
		if err != nil {
			if forget && !dryRun {
				if removeErr := RemoveSnapshot(ctx, repo, id); removeErr != nil {
					return nil, removeErr
				}
				results = append(results, RepairedSnapshot{SourceID: id.String(), Changed: true, Removed: true})
				continue
			}
			return nil, err
		}
		if sn.Tree == nil {
			if forget && !dryRun {
				if removeErr := RemoveSnapshot(ctx, repo, id); removeErr != nil {
					return nil, removeErr
				}
				results = append(results, RepairedSnapshot{SourceID: id.String(), Changed: true, Removed: true})
				continue
			}
			results = append(results, RepairedSnapshot{SourceID: id.String()})
			continue
		}

		var newTree ID
		if dryRun {
			newTree, err = rewriter.RewriteTree(ctx, repo, dryRunBlobSaver{repo: repo}, "/", *sn.Tree)
		} else {
			err = repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
				var rewriteErr error
				newTree, rewriteErr = rewriter.RewriteTree(ctx, repo, uploader, "/", *sn.Tree)
				return rewriteErr
			})
		}
		if err != nil {
			return nil, err
		}
		if newTree.IsNull() {
			if forget && !dryRun {
				if removeErr := RemoveSnapshot(ctx, repo, id); removeErr != nil {
					return nil, removeErr
				}
			}
			results = append(results, RepairedSnapshot{SourceID: id.String(), Changed: true, Removed: forget && !dryRun})
			continue
		}
		if newTree == *sn.Tree {
			results = append(results, RepairedSnapshot{SourceID: id.String()})
			continue
		}
		result := RepairedSnapshot{SourceID: id.String(), Changed: true}
		if !dryRun {
			original := id
			sn.Original = &original
			sn.Tree = &newTree
			newID, saveErr := SaveSnapshot(ctx, repo, sn)
			if saveErr != nil {
				return nil, saveErr
			}
			result.NewID = newID.String()
			if forget {
				if removeErr := RemoveSnapshot(ctx, repo, id); removeErr != nil {
					return nil, removeErr
				}
				result.Removed = true
			}
		}
		results = append(results, result)
	}
	return results, nil
}

// ============================================================================
// LOCKS
// ============================================================================

type Lock = repository.Lock

func NewExclusiveLock(ctx context.Context, repo *Repository, retrySleep time.Duration, p *progress.Counter) (func(), context.Context, error) {
	return repository.LockRepo(ctx, repo, true, retrySleep, func(string) {}, func(string, ...any) {})
}

// NewSharedLock acquires a non-exclusive repository lock. Use it for readers
// such as snapshot copy so prune/forget cannot remove data underneath them.
func NewSharedLock(ctx context.Context, repo *Repository, retrySleep time.Duration, p *progress.Counter) (func(), context.Context, error) {
	return repository.LockRepo(ctx, repo, false, retrySleep, func(string) {}, func(string, ...any) {})
}

// RepositoryLockRetry is how long LockRepository waits for a conflicting
// lock to be released before giving up.
const RepositoryLockRetry = time.Second

// LockRepository takes the lock the restic CLI takes for the same kind of
// operation: exclusive for operations that delete data (prune, forget, key
// removal), non-exclusive for operations that only add data (backup, rewrite
// without forget, copy). Without these locks a prune on one device can delete
// packs that a concurrent backup on another device has just deduplicated
// against, leaving the new snapshot referencing missing data.
//
// Use the returned context for the locked work: it is cancelled if the lock
// cannot be refreshed. Call unlock when done.
func LockRepository(ctx context.Context, repo *Repository, exclusive bool) (unlock func(), lockedCtx context.Context, err error) {
	return repository.LockRepo(ctx, repo, exclusive, RepositoryLockRetry, func(string) {}, func(string, ...any) {})
}

func IsAlreadyLocked(err error) bool {
	return repository.IsAlreadyLocked(err)
}

type PruneOptions = repository.PruneOptions
type PrunePlan = repository.PrunePlan
type FindBlobSet = restic.FindBlobSet

type ProgressPrinter = restic.Printer

// NoopPrinter implements ProgressPrinter but does nothing
type NoopPrinter struct{}

func (n NoopPrinter) NewCounter(description string) restic.Counter { return restic.NoopCounter }
func (n NoopPrinter) NewCounterTerminalOnly(description string) restic.Counter {
	return restic.NoopCounter
}
func (n NoopPrinter) E(msg string, args ...interface{})  {}
func (n NoopPrinter) S(msg string, args ...interface{})  {}
func (n NoopPrinter) PT(msg string, args ...interface{}) {}
func (n NoopPrinter) P(msg string, args ...interface{})  {}
func (n NoopPrinter) V(msg string, args ...interface{})  {}
func (n NoopPrinter) VV(msg string, args ...interface{}) {}

func NewNoopPrinter() NoopPrinter {
	return NoopPrinter{}
}

// PlanPrune wraps repository.PlanPrune, which needs the concrete
// *Repository. Run the returned plan with (*PrunePlan).Execute.
func PlanPrune(ctx context.Context, opts PruneOptions, repo restic.Repository,
	getUsedBlobs func(ctx context.Context, repo restic.Repository, usedBlobs FindBlobSet) error,
	p ProgressPrinter) (*PrunePlan, error) {

	repoConcrete, ok := repo.(*repository.Repository)
	if !ok {
		return nil, fmt.Errorf("prune requires *Repository, got %T", repo)
	}
	return repository.PlanPrune(ctx, opts, repoConcrete, getUsedBlobs, p)
}

func IsNoKeyFound(err error) bool {
	return stderrors.Is(err, repository.ErrNoKeyFound)
}

func IsNoRepository(err error) bool {
	return stderrors.Is(err, global.ErrNoRepository)
}

func IsCanceled(err error) bool {
	return stderrors.Is(err, context.Canceled)
}

// ============================================================================
// HELPER WRAPPERS
// (Because some methods are attached to internal structs we can't fully alias)
// ============================================================================

func LoadSnapshot(ctx context.Context, repo restic.Repository, id ID) (*Snapshot, error) {
	return data.LoadSnapshot(ctx, restic.LoaderUnpacked(repo), id)
}

func FindLatestSnapshot(ctx context.Context, repo restic.Repository, paths []string, tags []string) (*Snapshot, error) {
	filter := &data.SnapshotFilter{Paths: paths}
	if len(tags) > 0 {
		filter.Tags = data.TagLists{tags}
	}

	sn, _, err := filter.FindLatest(ctx, repo, repo, "latest")
	return sn, err
}

func FindLatestSnapshotForBackup(ctx context.Context, repo restic.Repository, paths []string, tags []string, hostname string, timeStampLimit time.Time) (*Snapshot, error) {
	filter := &data.SnapshotFilter{
		TimestampLimit: timeStampLimit,
		Paths:          paths,
	}
	if hostname != "" {
		filter.Hosts = []string{hostname}
	}
	if len(tags) > 0 {
		filter.Tags = data.TagLists{tags}
	}

	sn, _, err := filter.FindLatest(ctx, repo, repo, "latest")
	if stderrors.Is(err, data.ErrNoSnapshotFound) {
		return nil, nil
	}

	return sn, err
}

// Tree/Node types and LoadTree for snapshot browsing.
// upstream data.LoadTree now returns an iterator, so we materialize it back
// into the old Tree{Nodes} shape expected by the FFI layer.
type Tree struct {
	Nodes []*Node
}

type NodeType = data.NodeType

var (
	NodeTypeFile      = data.NodeTypeFile
	NodeTypeDir       = data.NodeTypeDir
	NodeTypeSymlink   = data.NodeTypeSymlink
	NodeTypeDev       = data.NodeTypeDev
	NodeTypeCharDev   = data.NodeTypeCharDev
	NodeTypeFifo      = data.NodeTypeFifo
	NodeTypeSocket    = data.NodeTypeSocket
	NodeTypeIrregular = data.NodeTypeIrregular
	NodeTypeInvalid   = data.NodeTypeInvalid
)

type Node = data.Node

func LoadTree(ctx context.Context, repo restic.Repository, id ID) (*Tree, error) {
	it, err := data.LoadTree(ctx, repo, id)
	if err != nil {
		return nil, err
	}

	nodes := make([]*Node, 0)
	for item := range it {
		if item.Error != nil {
			return nil, item.Error
		}
		if item.Node != nil {
			nodes = append(nodes, item.Node)
		}
	}

	return &Tree{Nodes: nodes}, nil
}

// DataBlob is the blob type for file content
var DataBlob = restic.DataBlob

// TreeBlob is the blob type for tree metadata
var TreeBlob = restic.TreeBlob

// FindUsedBlobs adds all blobs reachable from treeIDs to blobs. Trees are
// loaded in parallel.
func FindUsedBlobs(ctx context.Context, repo restic.Repository, treeIDs []ID, blobs FindBlobSet, p *progress.Counter) error {
	return data.FindUsedBlobs(ctx, repo, treeIDs, blobs, counterOrNoop(p))
}

// PackSizes returns the size of every pack file, computed from the loaded
// in-memory index. Unlike listing the backend this costs no requests.
func PackSizes(ctx context.Context, repo *Repository) (map[ID]int64, error) {
	return pack.Size(ctx, repo, false)
}

// CollectUsedBlobs adds every blob referenced by any snapshot to usedBlobs.
// It is the getUsedBlobs callback PlanPrune expects and mirrors `restic
// prune`: snapshots are loaded in parallel, and an unreadable snapshot is a
// fatal error so prune never deletes the data of a snapshot it could not read.
func CollectUsedBlobs(ctx context.Context, repo restic.Repository, usedBlobs FindBlobSet) error {
	var trees restic.IDs
	err := data.ForAllSnapshots(ctx, repo, repo, nil, func(id restic.ID, sn *data.Snapshot, err error) error {
		if err != nil {
			return fmt.Errorf("load snapshot %s: %w", id.Str(), err)
		}
		if sn.Tree == nil {
			return fmt.Errorf("snapshot %s has no tree", id.Str())
		}
		trees = append(trees, *sn.Tree)
		return nil
	})
	if err != nil {
		return err
	}
	return data.FindUsedBlobs(ctx, repo, trees, usedBlobs, restic.NoopCounter)
}

func ParseID(s string) (ID, error) {
	return restic.ParseID(s)
}

// Constants
var SnapshotFile = backend.SnapshotFile
var PackFile = backend.PackFile
var KeyFile = backend.KeyFile
var LockFile = backend.LockFile
var IndexFile = backend.IndexFile
var ConfigFile = backend.ConfigFile
var InvalidBlob = restic.InvalidBlob
var NumBlobTypes = restic.NumBlobTypes

// ResticFileType converts a backend file type to the corresponding restic
// file type. The numeric values are identical; the types differ to keep the
// backend package decoupled from restic.
func ResticFileType(t FileType) restic.FileType {
	return restic.FileType(t)
}

// Interface Aliases
type MasterIndex = index.MasterIndex
type Blob = pack.Blob
type BlobHandle = restic.BlobHandle
type PackedBlob = pack.PackedBlob
type BlobSet = restic.BlobSet
type IDSet = restic.IDSet

// InitRepository creates a new repository on be using the latest stable
// repository format (version 2, compression support) and a fresh random
// chunker polynomial.
func InitRepository(ctx context.Context, be Backend, pwd string) (*Repository, error) {
	repo, err := repository.New(be, repository.Options{})
	if err != nil {
		return nil, err
	}
	if err := repo.Init(ctx, restic.StableRepoVersion, pwd, nil); err != nil {
		return nil, err
	}
	return repo, nil
}

// Check (re)loads the index, then runs the snapshot tree structure check, and
// returns the first error found. It does not read pack data.
func Check(ctx context.Context, repo restic.Repository) error {
	chk := NewChecker(repo, false)
	if _, errs := chk.LoadIndex(ctx, nil); len(errs) > 0 {
		return errs[0]
	}
	if err := CheckerLoadSnapshots(ctx, chk, nil); err != nil {
		return err
	}

	// Cancel the checker as soon as the first error arrives; otherwise its
	// goroutine would block forever sending on errChan after we return.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error)
	go chk.Structure(ctx, restic.NoopCounter, errChan) // closes errChan

	var first error
	for err := range errChan {
		if err != nil && first == nil {
			first = err
			cancel()
		}
	}
	if first != nil {
		return first
	}
	return ctx.Err()
}

func NewIDSet() restic.IDSet {
	return restic.NewIDSet()
}

// ============================================================================
// LOCKS
// ============================================================================

// RemoveStaleRepositoryLocks removes only locks that restic considers stale.
func RemoveStaleRepositoryLocks(ctx context.Context, repo *Repository) (uint, error) {
	return repository.RemoveStaleLocks(ctx, repo)
}

// RemoveAllRepositoryLocks forcefully removes every lock file.
func RemoveAllRepositoryLocks(ctx context.Context, repo *Repository) (uint, error) {
	return repository.RemoveAllLocks(ctx, repo)
}

// RepairRepositoryIndex rebuilds the repository index from pack files.
func RepairRepositoryIndex(ctx context.Context, repo *Repository, readAllPacks bool) error {
	return repository.RepairIndex(
		ctx,
		repo,
		repository.RepairIndexOptions{ReadAllPacks: readAllPacks},
		NewNoopPrinter(),
	)
}

// ============================================================================
// SNAPSHOT COPY
// ============================================================================

type CopiedSnapshot struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
}

// CopyStats reports snapshot copy progress. DataBlobs/TreeBlobs count blobs
// that had to be transferred; Skipped counts distinct blobs that the
// destination already had. PacksCopied/PacksTotal drive a determinate
// progress bar; PacksTotal grows as each snapshot's trees are scanned.
type CopyStats struct {
	Snapshots   int    `json:"snapshots"`
	DataBlobs   int    `json:"data_blobs"`
	TreeBlobs   int    `json:"tree_blobs"`
	Skipped     int    `json:"skipped"`
	Bytes       uint64 `json:"bytes"`
	PacksTotal  int    `json:"packs_total"`
	PacksCopied int    `json:"packs_copied"`
}

const (
	// Like `restic copy`, save snapshots once a batch has written at least
	// copyBatchPacks packs worth of data and has run for copyBatchMinDuration.
	// Batching bounds the work lost on cancellation without writing an index
	// file per snapshot.
	copyBatchPacks       = 100
	copyBatchMinDuration = time.Minute
)

// CopySnapshots copies the given snapshots (including all referenced trees and
// data blobs) from src into dst. It does not mutate the source repository.
func CopySnapshots(ctx context.Context, src, dst *Repository, snapshotIDs []ID) ([]CopiedSnapshot, CopyStats, error) {
	return CopySnapshotsWithProgress(ctx, src, dst, snapshotIDs, nil)
}

// CopySnapshotsWithProgress is CopySnapshots with a progress callback. The
// callback may run concurrently from worker goroutines (it is serialized by
// the adapter) and must be cheap and non-blocking.
//
// The indexes of both repositories must be loaded, and loaded after the
// caller acquired its locks: blobs already present in dst are not copied
// again, so a stale dst index could hide blobs removed by a concurrent prune.
//
// Like `restic copy`, trees are streamed in parallel and blobs are copied
// pack by pack via repository.CopyBlobs, so incremental copies only download
// the packs that hold missing blobs. Snapshots are saved only after the packs
// they reference have been flushed. On error, the snapshots saved by earlier
// batches are returned together with the error.
func CopySnapshotsWithProgress(ctx context.Context, src, dst *Repository, snapshotIDs []ID, onProgress func(CopyStats)) ([]CopiedSnapshot, CopyStats, error) {
	progress := &copyProgress{onChange: onProgress}
	copied := make([]CopiedSnapshot, 0, len(snapshotIDs))
	// Shared across snapshots: trees already scanned and blobs already
	// classified (copied or skipped) in this run.
	visitedTrees := src.NewAssociatedBlobSet()
	seenBlobs := src.NewAssociatedBlobSet()

	type pendingSnapshot struct {
		sn       *Snapshot
		sourceID ID
	}

	targetBatchBytes := uint64(dst.PackSize()) * copyBatchPacks
	remaining := snapshotIDs
	for len(remaining) > 0 {
		var batch []pendingSnapshot
		var batchBytes uint64
		started := time.Now()

		err := dst.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for len(remaining) > 0 && (batchBytes < targetBatchBytes || time.Since(started) < copyBatchMinDuration) {
				sourceID := remaining[0]
				sn, err := LoadSnapshot(ctx, src, sourceID)
				if err != nil {
					return fmt.Errorf("load source snapshot %s: %w", sourceID.Str(), err)
				}
				if sn.Tree == nil {
					return fmt.Errorf("snapshot %s has no tree", sourceID.Str())
				}
				size, err := copySnapshotTree(ctx, src, dst, uploader, visitedTrees, seenBlobs, *sn.Tree, progress)
				if err != nil {
					return fmt.Errorf("copy snapshot %s: %w", sourceID.Str(), err)
				}
				batch = append(batch, pendingSnapshot{sn: sn, sourceID: sourceID})
				batchBytes += size
				remaining = remaining[1:]
			}
			return nil
		})
		if err != nil {
			return copied, progress.get(), err
		}

		// WithBlobUploader has flushed every pack and the index by now, so
		// these snapshots can never point at uncommitted blobs.
		for _, item := range batch {
			sn := item.sn
			if sn.Original == nil {
				original := item.sourceID
				sn.Original = &original
			}
			sn.Parent = nil
			newID, err := SaveSnapshot(ctx, dst, sn)
			if err != nil {
				return copied, progress.get(), fmt.Errorf("save destination snapshot %s: %w", item.sourceID.Str(), err)
			}
			progress.update(func(s *CopyStats) { s.Snapshots++ })
			copied = append(copied, CopiedSnapshot{
				SourceID:      item.sourceID.String(),
				DestinationID: newID.String(),
			})
		}
	}
	return copied, progress.get(), nil
}

// copySnapshotTree schedules every blob reachable from root that dst lacks
// and copies them pack by pack. It returns the ciphertext size scheduled.
func copySnapshotTree(
	ctx context.Context,
	src, dst *Repository,
	uploader restic.BlobSaverWithAsync,
	visitedTrees, seenBlobs restic.AssociatedBlobSet,
	root ID,
	progress *copyProgress,
) (uint64, error) {
	toCopy := src.NewAssociatedBlobSet()
	packs := restic.NewIDSet()
	var (
		mu                            sync.Mutex
		dataBlobs, treeBlobs, skipped int
		scheduledBytes                uint64
		missing                       []restic.BlobHandle
	)

	enqueue := func(h restic.BlobHandle) {
		mu.Lock()
		defer mu.Unlock()
		if seenBlobs.Has(h) {
			return
		}
		seenBlobs.Insert(h)
		if _, ok := dst.LookupBlobSize(h); ok {
			skipped++
			return
		}
		locations := src.LookupBlob(h)
		if len(locations) == 0 {
			missing = append(missing, h)
			return
		}
		toCopy.Insert(h)
		for _, pb := range locations {
			packs.Insert(pb.PackID())
		}
		scheduledBytes += uint64(locations[0].CiphertextLength())
		if h.Type == restic.TreeBlob {
			treeBlobs++
		} else {
			dataBlobs++
		}
	}

	err := data.StreamTrees(ctx, src, restic.IDs{root}, restic.NoopCounter, func(treeID restic.ID) bool {
		// skip is called from a single goroutine.
		h := restic.BlobHandle{ID: treeID, Type: restic.TreeBlob}
		visited := visitedTrees.Has(h)
		visitedTrees.Insert(h)
		return visited
	}, func(treeID restic.ID, err error, nodes data.TreeNodeIterator) error {
		if err != nil {
			return fmt.Errorf("load tree %s: %w", treeID.Str(), err)
		}
		// Copy the raw tree blob so a serialization change cannot alter IDs.
		enqueue(restic.BlobHandle{ID: treeID, Type: restic.TreeBlob})
		for item := range nodes {
			if item.Error != nil {
				return item.Error
			}
			// StreamTrees recurses into subdirectories itself.
			for _, blobID := range item.Node.Content {
				enqueue(restic.BlobHandle{ID: blobID, Type: restic.DataBlob})
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(missing) > 0 {
		return 0, fmt.Errorf("%d blob(s) referenced by the snapshot are missing from the source repository (first: %v); run check/repair on the source first", len(missing), missing[0])
	}

	progress.update(func(s *CopyStats) {
		s.DataBlobs += dataBlobs
		s.TreeBlobs += treeBlobs
		s.Skipped += skipped
		s.Bytes += scheduledBytes
		s.PacksTotal += len(packs)
	})

	if err := repository.CopyBlobs(ctx, src, dst, uploader, packs, toCopy, progress.packCounter(), nil); err != nil {
		return 0, err
	}
	// CopyBlobs deletes each blob from toCopy once it has been saved.
	if left := toCopy.Len(); left > 0 {
		return 0, fmt.Errorf("%d blob(s) could not be found in their source packs; run check/repair on the source first", left)
	}
	return scheduledBytes, nil
}

// copyProgress serializes CopyStats updates from the copy workers.
type copyProgress struct {
	mu       sync.Mutex
	stats    CopyStats
	onChange func(CopyStats)
}

func (p *copyProgress) update(fn func(*CopyStats)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(&p.stats)
	if p.onChange != nil {
		p.onChange(p.stats)
	}
}

func (p *copyProgress) get() CopyStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *copyProgress) packCounter() restic.Counter { return copyPackCounter{p} }

// copyPackCounter adapts copyProgress to the restic.Counter that CopyBlobs
// increments once per pack. SetMax is ignored because PacksTotal accumulates
// across snapshots.
type copyPackCounter struct{ p *copyProgress }

func (c copyPackCounter) Add(n uint64) {
	c.p.update(func(s *CopyStats) { s.PacksCopied += int(n) })
}
func (c copyPackCounter) SetMax(uint64) {}
func (c copyPackCounter) Get() (uint64, uint64) {
	s := c.p.get()
	return uint64(s.PacksCopied), uint64(s.PacksTotal)
}
func (c copyPackCounter) Done() {}

// ============================================================================
// KEY HINT OPTIMIZATION
// ============================================================================

// RepositoryKeyInfo is the non-secret metadata of a repository password key.
type RepositoryKeyInfo struct {
	ID       string    `json:"id"`
	Current  bool      `json:"current"`
	Username string    `json:"username"`
	Hostname string    `json:"hostname"`
	Created  time.Time `json:"created"`
}

// ListRepositoryKeys enumerates the repository's password keys. The key list
// contains metadata only; encrypted key material never leaves the repository.
func ListRepositoryKeys(ctx context.Context, repo *Repository) ([]RepositoryKeyInfo, error) {
	keys := make([]RepositoryKeyInfo, 0)
	currentID := RepositoryKeyID(repo)
	err := repo.List(ctx, restic.KeyFile, func(id restic.ID, size int64) error {
		key, err := repository.LoadKey(ctx, repo, id)
		if err != nil {
			return nil // one unreadable key should not hide the others
		}
		keys = append(keys, RepositoryKeyInfo{
			ID:       id.String(),
			Current:  id == currentID,
			Username: key.Username,
			Hostname: key.Hostname,
			Created:  key.Created,
		})
		return nil
	})
	return keys, err
}

// AddRepositoryKey adds another password key to the repository. The caller is
// responsible for confirming the new password unlocks the repository.
func AddRepositoryKey(ctx context.Context, repo *Repository, password, username, hostname string) (string, error) {
	key, err := repository.AddKey(ctx, repo, password, username, hostname, repo.Key())
	if err != nil {
		return "", err
	}
	// Verify the new password actually unlocks the key before reporting success.
	if err := repo.SearchKey(ctx, password, 1, key.ID().String()); err != nil {
		_ = repository.RemoveKey(ctx, repo, key.ID())
		return "", err
	}
	return key.ID().String(), nil
}

// RemoveRepositoryKey removes a key by ID/prefix. It refuses to remove the last
// remaining key.
func RemoveRepositoryKey(ctx context.Context, repo *Repository, idPrefix string) error {
	id, err := restic.Find(ctx, repo, restic.KeyFile, idPrefix)
	if err != nil {
		return err
	}
	var count int
	if err := repo.List(ctx, restic.KeyFile, func(restic.ID, int64) error {
		count++
		return nil
	}); err != nil {
		return err
	}
	if count <= 1 {
		return stderrors.New("refusing to remove the last repository key")
	}
	if id == repo.KeyID() {
		return stderrors.New("refusing to remove the key currently used to access the repository")
	}
	return repository.RemoveKey(ctx, repo, id)
}

// RepositoryKeyID returns the key ID of the currently-loaded key.
// Cache this after a successful SearchKey and pass it as keyHint on
// subsequent opens to skip the expensive key-listing loop.
func RepositoryKeyID(repo *Repository) ID {
	return repo.KeyID()
}

// ============================================================================
// PARALLEL TREE STREAMING
// ============================================================================

// TreeNodeIterator is an alias for the streaming tree node iterator.
type TreeNodeIterator = data.TreeNodeIterator

// NodeOrError wraps a node or error from a tree iterator.
type NodeOrError = data.NodeOrError

// StreamTrees loads trees and their subtrees in parallel using
// repo.Connections()+GOMAXPROCS worker goroutines.
// The skip callback is single-threaded; process is called from workers and
// MUST consume the iterator fully.
func StreamTrees(
	ctx context.Context,
	repo *Repository,
	trees []ID,
	p *progress.Counter,
	skip func(tree ID) bool,
	process func(id ID, err error, nodes TreeNodeIterator) error,
) error {
	return data.StreamTrees(ctx, repo, trees, counterOrNoop(p), skip, process)
}

// counterOrNoop converts a possibly nil *progress.Counter into a usable
// restic.Counter. Passing a nil *progress.Counter straight through would wrap
// it in a non-nil interface, and restic calls Add on it unconditionally, which
// panics (crashing the host process from a worker goroutine).
func counterOrNoop(p *progress.Counter) restic.Counter {
	if p == nil {
		return restic.NoopCounter
	}
	return p
}

// ============================================================================
// DIFF
// ============================================================================

// DiffChange represents a single change between two snapshots.
type DiffChange struct {
	Path     string `json:"path"`
	Modifier string `json:"modifier"` // "+", "-", "M", "U", "T", "?"
}

// DiffStat collects stats for all types of items. DataBlobs, TreeBlobs and
// Bytes are reserved for parity with `restic diff` and are not computed yet.
type DiffStat struct {
	Files     int    `json:"files"`
	Dirs      int    `json:"dirs"`
	Others    int    `json:"others"`
	DataBlobs int    `json:"data_blobs"`
	TreeBlobs int    `json:"tree_blobs"`
	Bytes     uint64 `json:"bytes"`
}

// DiffStats holds aggregate diff statistics.
type DiffStats struct {
	ChangedFiles int      `json:"changed_files"`
	Added        DiffStat `json:"added"`
	Removed      DiffStat `json:"removed"`
}

func addDiffStatNode(s *DiffStat, node *Node) {
	if node == nil {
		return
	}
	switch node.Type {
	case data.NodeTypeFile:
		s.Files++
	case data.NodeTypeDir:
		s.Dirs++
	default:
		s.Others++
	}
}

// diffPrintDir recursively collects all changes for a directory added or removed entirely.
func diffPrintDir(ctx context.Context, repo restic.BlobLoader, mode string, stats *DiffStat, prefix string, id ID, changes *[]DiffChange) error {
	tree, err := data.LoadTree(ctx, repo, id)
	if err != nil {
		return err
	}
	for item := range tree {
		if item.Error != nil {
			return item.Error
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		node := item.Node
		name := path.Join(prefix, node.Name)
		if node.Type == data.NodeTypeDir {
			name += "/"
		}
		*changes = append(*changes, DiffChange{Path: name, Modifier: mode})
		addDiffStatNode(stats, node)
		if node.Type == data.NodeTypeDir && node.Subtree != nil {
			if err := diffPrintDir(ctx, repo, mode, stats, name, *node.Subtree, changes); err != nil {
				return diffSubtreeErr(name, err)
			}
		}
	}
	return ctx.Err()
}

// diffSubtreeErr keeps the path of the first unreadable subtree. Upstream
// `restic diff` prints such errors and continues; an API has no place to
// print, and silently dropping the subtree would report a wrong diff.
func diffSubtreeErr(name string, err error) error {
	if stderrors.Is(err, context.Canceled) {
		return err
	}
	var pathErr *diffPathError
	if stderrors.As(err, &pathErr) {
		return err
	}
	return &diffPathError{path: name, err: err}
}

type diffPathError struct {
	path string
	err  error
}

func (e *diffPathError) Error() string { return fmt.Sprintf("diff %s: %v", e.path, e.err) }
func (e *diffPathError) Unwrap() error { return e.err }

// diffTree recursively compares two trees and collects changes.
func diffTree(ctx context.Context, repo restic.BlobLoader, stats *DiffStats, prefix string, id1, id2 ID, changes *[]DiffChange, showMetadata bool) error {
	tree1, err := data.LoadTree(ctx, repo, id1)
	if err != nil {
		return err
	}
	tree2, err := data.LoadTree(ctx, repo, id2)
	if err != nil {
		return err
	}

	for dt := range data.DualTreeIterator(tree1, tree2) {
		if dt.Error != nil {
			return dt.Error
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		node1 := dt.Tree1
		node2 := dt.Tree2

		var name string
		if node1 != nil {
			name = node1.Name
		} else {
			name = node2.Name
		}

		switch {
		case node1 != nil && node2 != nil:
			fullName := path.Join(prefix, name)
			mod := ""

			if node1.Type != node2.Type {
				mod += "T"
			}

			if node2.Type == data.NodeTypeDir {
				fullName += "/"
			}

			if node1.Type == data.NodeTypeFile &&
				node2.Type == data.NodeTypeFile &&
				!slices.Equal(node1.Content, node2.Content) {
				mod += "M"
				stats.ChangedFiles++

				// bitrot detection
				node1Copy := *node1
				node2Copy := *node2
				node1Copy.Content = nil
				node2Copy.Content = nil
				if node1Copy.Equals(node2Copy) {
					mod += "?"
				}
			} else if showMetadata && !node1.Equals(*node2) {
				mod += "U"
			}

			if mod != "" {
				*changes = append(*changes, DiffChange{Path: fullName, Modifier: mod})
			}

			if node1.Type == data.NodeTypeDir && node2.Type == data.NodeTypeDir {
				if node1.Subtree != nil && node2.Subtree != nil && !node1.Subtree.Equal(*node2.Subtree) {
					if err := diffTree(ctx, repo, stats, fullName, *node1.Subtree, *node2.Subtree, changes, showMetadata); err != nil {
						return diffSubtreeErr(fullName, err)
					}
				}
			}

		case node1 != nil && node2 == nil:
			fullName := path.Join(prefix, name)
			if node1.Type == data.NodeTypeDir {
				fullName += "/"
			}
			*changes = append(*changes, DiffChange{Path: fullName, Modifier: "-"})
			addDiffStatNode(&stats.Removed, node1)
			if node1.Type == data.NodeTypeDir && node1.Subtree != nil {
				if err := diffPrintDir(ctx, repo, "-", &stats.Removed, fullName, *node1.Subtree, changes); err != nil {
					return diffSubtreeErr(fullName, err)
				}
			}

		case node1 == nil && node2 != nil:
			fullName := path.Join(prefix, name)
			if node2.Type == data.NodeTypeDir {
				fullName += "/"
			}
			*changes = append(*changes, DiffChange{Path: fullName, Modifier: "+"})
			addDiffStatNode(&stats.Added, node2)
			if node2.Type == data.NodeTypeDir && node2.Subtree != nil {
				if err := diffPrintDir(ctx, repo, "+", &stats.Added, fullName, *node2.Subtree, changes); err != nil {
					return diffSubtreeErr(fullName, err)
				}
			}
		}
	}

	return ctx.Err()
}

// ComputeDiff compares two snapshots and returns all changes + aggregate stats.
func ComputeDiff(ctx context.Context, repo *Repository, sn1, sn2 *Snapshot) ([]DiffChange, DiffStats, error) {
	if sn1.Tree == nil {
		return nil, DiffStats{}, stderrors.New("snapshot 1 has nil tree")
	}
	if sn2.Tree == nil {
		return nil, DiffStats{}, stderrors.New("snapshot 2 has nil tree")
	}

	var changes []DiffChange
	stats := DiffStats{}

	err := diffTree(ctx, repo, &stats, "/", *sn1.Tree, *sn2.Tree, &changes, true)
	if err != nil {
		return nil, DiffStats{}, err
	}

	return changes, stats, nil
}

// ============================================================================
// SNAPSHOT GROUPING
// ============================================================================

type SnapshotGroupByOptions = data.SnapshotGroupByOptions
type SnapshotGroupKey = data.SnapshotGroupKey
type Snapshots = data.Snapshots
type ExpirePolicy = data.ExpirePolicy
type KeepReason = data.KeepReason

// GroupSnapshots groups snapshots by the given criteria.
func GroupSnapshots(snapshots Snapshots, groupBy SnapshotGroupByOptions) (map[string]Snapshots, bool, error) {
	return data.GroupSnapshots(snapshots, groupBy)
}

// ApplyPolicy returns the snapshots to keep and remove according to the policy.
func ApplyPolicy(list Snapshots, p ExpirePolicy) (keep, remove Snapshots, reasons []KeepReason) {
	return data.ApplyPolicy(list, p)
}

// ============================================================================
// FORGET (REMOVE SNAPSHOT)
// ============================================================================

// RemoveSnapshot removes a single snapshot from the repository.
func RemoveSnapshot(ctx context.Context, repo *Repository, id ID) error {
	return repo.RemoveUnpacked(ctx, restic.WriteableSnapshotFile, id)
}

// SaveSnapshot saves a (possibly modified) snapshot and returns its new ID.
func SaveSnapshot(ctx context.Context, repo *Repository, sn *Snapshot) (ID, error) {
	return data.SaveSnapshot(ctx, repo, sn)
}

// ForAllSnapshots iterates all snapshots in the repository.
func ForAllSnapshots(ctx context.Context, repo *Repository, excludeIDs IDSet, fn func(ID, *Snapshot, error) error) error {
	return data.ForAllSnapshots(ctx, repo, repo, excludeIDs, fn)
}

// ============================================================================
// REWRITE (TREE REWRITER)
// ============================================================================

type TreeRewriter = walker.TreeRewriter
type RewriteOpts = walker.RewriteOpts
type NodeRewriteFunc = walker.NodeRewriteFunc
type SnapshotSize = walker.SnapshotSize
type QueryRewrittenSizeFunc = walker.QueryRewrittenSizeFunc

func NewTreeRewriter(opts RewriteOpts) *TreeRewriter {
	return walker.NewTreeRewriter(opts)
}

func NewSnapshotSizeRewriter(rewriteNode NodeRewriteFunc, keepEmpty walker.NodeKeepEmptyDirectoryFunc) (*TreeRewriter, QueryRewrittenSizeFunc) {
	return walker.NewSnapshotSizeRewriter(rewriteNode, keepEmpty)
}

// RewriteSnapshot rewrites a snapshot's tree, excluding the given paths.
// Returns the new snapshot ID. The original snapshot is NOT removed.
func RewriteSnapshot(ctx context.Context, repo *Repository, sn *Snapshot, excludePaths []string) (ID, *SnapshotSize, error) {
	if sn == nil || sn.Tree == nil {
		return ID{}, nil, stderrors.New("snapshot has no tree")
	}
	excludeSet := make(map[string]bool, len(excludePaths))
	for _, p := range excludePaths {
		excludeSet[p] = true
	}

	rewriteNode := func(node *Node, nodePath string) *Node {
		if excludeSet[nodePath] {
			return nil // remove this node
		}
		return node
	}

	rewriter, querySize := NewSnapshotSizeRewriter(rewriteNode, func(_ string) bool { return false })

	var newTreeID ID
	err := repo.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		var rewriteErr error
		newTreeID, rewriteErr = rewriter.RewriteTree(ctx, repo, uploader, "/", *sn.Tree)
		return rewriteErr
	})
	if err != nil {
		return ID{}, nil, err
	}

	if newTreeID.IsNull() {
		return ID{}, nil, stderrors.New("rewrite resulted in empty snapshot")
	}

	// Create a copy of the snapshot with the new tree
	newSn := *sn
	newSn.Tree = &newTreeID
	if sn.ID() != nil {
		original := *sn.ID()
		newSn.Original = &original
	}

	newID, err := SaveSnapshot(ctx, repo, &newSn)
	if err != nil {
		return ID{}, nil, err
	}

	size := querySize()
	return newID, &size, nil
}

// ============================================================================
// RESTORE PROGRESS
// ============================================================================

type RestoreProgress = restoreui.Progress
type RestoreState = restoreui.State
type RestoreProgressPrinter = restoreui.ProgressPrinter
type RestoreItemAction = restorer.ItemAction
type ProgressCounter = restic.Counter

// NoopCounter is a Counter that discards all updates.
var NoopCounter restic.Counter = restic.NoopCounter

func NewRestoreProgress(printer RestoreProgressPrinter, interval time.Duration) *RestoreProgress {
	// NewProgress no longer accepts an interval; it computes one internally.
	// The interval argument is kept for call compatibility but ignored.
	return restoreui.NewProgress(printer, false, false, false)
}

// SelectFilter is the type for chooseing which items to restore.
type SelectFilter = func(item string, isDir bool) (selectedForRestore bool, childMayBeSelected bool)

// Restorer type alias
type Restorer = restorer.Restorer
