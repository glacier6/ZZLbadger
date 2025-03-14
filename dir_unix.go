//go:build !windows && !plan9 && !js && !wasip1
// +build !windows,!plan9,!js,!wasip1

/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	"github.com/dgraph-io/badger/v4/y"
)

// directoryLockGuard holds a lock on a directory and a pid file inside.  The pid file isn't part
// of the locking mechanism, it's just advisory.
type directoryLockGuard struct {
	// File handle on the directory, which we've flocked.
	f *os.File
	// The absolute path to our pid file.
	path string
	// Was this a shared lock for a read-only database?
	readOnly bool
}

// acquireDirectoryLock gets a lock on the directory (using flock). If
// this is not read-only, it will also write our pid to
// dirPath/pidFileName for convenience.
// acquireDirectoryLock获取目录的锁（使用flock）。如果这不是只读的，为了方便起见，它还会将我们的pid写入dirPath/pidFileName。
func acquireDirectoryLock(dirPath string, pidFileName string, readOnly bool) (
	*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absPidFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, y.Wrapf(err, "cannot get absolute path for pid lock file")
	}
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, y.Wrapf(err, "cannot open directory %q", dirPath)
	}
	opts := unix.LOCK_EX | unix.LOCK_NB
	if readOnly {
		opts = unix.LOCK_SH | unix.LOCK_NB
	}

	err = unix.Flock(int(f.Fd()), opts)
	if err != nil {
		f.Close()
		return nil, y.Wrapf(err,
			"Cannot acquire directory lock on %q.  Another process is using this Badger database.",
			dirPath)
	}

	if !readOnly {
		// Yes, we happily overwrite a pre-existing pid file.  We're the
		// only read-write badger process using this directory.
		err = os.WriteFile(absPidFilePath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0666)
		if err != nil {
			f.Close()
			return nil, y.Wrapf(err,
				"Cannot write pid file %q", absPidFilePath)
		}
	}
	return &directoryLockGuard{f, absPidFilePath, readOnly}, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() error {
	var err error
	if !guard.readOnly {
		// It's important that we remove the pid file first.
		err = os.Remove(guard.path)
	}

	if closeErr := guard.f.Close(); err == nil {
		err = closeErr
	}
	guard.path = ""
	guard.f = nil

	return err
}

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) { return os.Open(path) }

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return y.Wrapf(err, "While opening directory: %s.", dir)
	}

	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return y.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return y.Wrapf(closeErr, "While closing directory: %s.", dir)
}
