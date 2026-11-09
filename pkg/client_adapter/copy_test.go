package client_adapter

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend/local"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func TestCopySnapshotsLocal(t *testing.T) {
	ctx := context.Background()
	srcPath := t.TempDir()
	dstPath := t.TempDir()

	srcBackend, err := local.Create(ctx, local.Config{Path: srcPath, Connections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dstBackend, err := local.Create(ctx, local.Config{Path: dstPath, Connections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	src, err := InitRepository(ctx, srcBackend, "source-password")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := InitRepository(ctx, dstBackend, "dest-password")
	if err != nil {
		t.Fatal(err)
	}

	var dataID, treeID, snapshotID restic.ID
	err = src.WithBlobUploader(ctx, func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		var err error
		dataID, _, _, err = uploader.SaveBlob(ctx, restic.DataBlob, []byte("hello copy"), restic.ID{}, false)
		if err != nil {
			return err
		}
		node := &data.Node{
			Name:    "file.txt",
			Type:    data.NodeTypeFile,
			Mode:    0o644,
			Content: restic.IDs{dataID},
		}
		treeJSON, err := json.Marshal(map[string]interface{}{"nodes": []*data.Node{node}})
		if err != nil {
			return err
		}
		treeID, _, _, err = uploader.SaveBlob(ctx, restic.TreeBlob, treeJSON, restic.ID{}, false)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshotID, err = SaveSnapshot(ctx, src, &data.Snapshot{
		Time:     time.Now(),
		Tree:     &treeID,
		Paths:    []string{"/source"},
		Hostname: "test-host",
	})
	if err != nil {
		t.Fatal(err)
	}

	copied, stats, err := CopySnapshots(ctx, src, dst, []ID{snapshotID})
	if err != nil {
		t.Fatal(err)
	}
	if len(copied) != 1 || stats.Snapshots != 1 || stats.DataBlobs != 1 || stats.TreeBlobs != 1 {
		t.Fatalf("copied = %v, stats = %+v", copied, stats)
	}

	copiedID, err := restic.ParseID(copied[0].DestinationID)
	if err != nil {
		t.Fatal(err)
	}
	sn, err := LoadSnapshot(ctx, dst, copiedID)
	if err != nil {
		t.Fatal(err)
	}
	if sn.Original == nil || sn.Original.String() != snapshotID.String() {
		t.Fatalf("original = %v, want %s", sn.Original, snapshotID)
	}
	tree, err := LoadTree(ctx, dst, *sn.Tree)
	if err != nil {
		t.Fatal(err)
	}
	if len(tree.Nodes) != 1 || tree.Nodes[0].Name != "file.txt" {
		t.Fatalf("tree nodes = %+v", tree.Nodes)
	}
	got, err := dst.LoadBlob(ctx, restic.BlobHandle{ID: tree.Nodes[0].Content[0], Type: restic.DataBlob}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello copy" {
		t.Fatalf("content = %q", got)
	}
	_ = os.RemoveAll(srcPath)
}
