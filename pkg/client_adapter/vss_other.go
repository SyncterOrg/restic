//go:build !windows

package client_adapter

import stderrors "errors"

func NewLocalVSSFS() (FileSystem, func(), error) {
	return nil, nil, stderrors.New("VSS is only available on Windows")
}
