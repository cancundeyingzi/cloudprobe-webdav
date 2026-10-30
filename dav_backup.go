package main

import "context"

// Absence used for rollback must come from the native namespace, never a
// read-only fallback or the short-lived DAV directory cache.
func (b davBackup) nativeDrive() davDrive {
	d := b.mount.drive
	for {
		fallback, ok := d.(*backupDAVDrive)
		if !ok {
			return d
		}
		d = fallback.primary
	}
}

func (b davBackup) snapshot(ctx context.Context) (target, backup *davEntry, err error) {
	entries, err := b.nativeDrive().List(directoryLogContext(ctx), b.parent)
	if err != nil {
		return nil, nil, err
	}
	target, err = uniqueNamedEntry(entries, b.entry.Name)
	if err == nil {
		backup, err = uniqueNamedEntry(entries, b.backupName)
	}
	return target, backup, err
}
