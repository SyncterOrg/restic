//go:build js && wasm

package repository

// processExists intentionally avoids PID-based stale-lock detection in the
// browser runtime where there is no meaningful host process model. Locks are
// therefore never considered stale due to a missing process.
func (l *lockHandle) processExists() bool {
	return true
}
