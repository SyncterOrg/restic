//go:build windows

package client_adapter

import "github.com/restic/restic/internal/fs"

// NewLocalVSSFS returns a VSS-backed local filesystem and its cleanup function.
func NewLocalVSSFS() (FileSystem, func(), error) {
	vss := fs.NewLocalVss(
		func(string, error) {},
		func(string, ...any) {},
		fs.VSSConfig{},
	)
	return vss, vss.DeleteSnapshots, nil
}
