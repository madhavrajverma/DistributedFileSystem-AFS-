package afs

import (
	"os"
	"sync"
)

// entry we have fetched from server
type cacheEntry struct {
	localPath     string
	cachedVersion int64 // version we have locally

}

type openEntry struct {
	localFd        *os.File
	dirty          bool
	serverHandleID int64
	path           string
}

type cacheManager struct {
	mu sync.Mutex

	cacheDir string
	//path -> cache metadata
	cached map[string]*cacheEntry

	//clientHandleID -> open file state
	openFiles map[int64]*openEntry

	nextHandle int64
}

// cacheDir is where the fetched file are stored locally
func newCacheManager(cacheDir string) (*cacheManager, error) {

	// create cache directory if it does not exist
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, err
	}

	return &cacheManager{
		cacheDir:  cacheDir,
		cached:    make(map[string]*cacheEntry),
		openFiles: make(map[int64]*openEntry),
	}, nil
}

func (cm *cacheManager) isCached(path string) (int64, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	entry, exist := cm.cached[path]

	if !exist {
		return 0, false
	}

	return entry.cachedVersion, true
}

func (cm *cacheManager) localPath(path string) string {
	return cm.cacheDir + "/" + path
}

func (cm *cacheManager) storeCacheEntry(path string, version int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.cached[path] = &cacheEntry{
		localPath:     cm.localPath(path),
		cachedVersion: version,
	}
}

func (cm *cacheManager) updateVersion(path string, version int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if entry, exists := cm.cached[path]; exists {
		entry.cachedVersion = version
	}
}

// returns clinet Handle ID
func (cm *cacheManager) openFile(path string, fd *os.File, serverHandleID int64) int64 {

	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.nextHandle++
	id := cm.nextHandle

	cm.openFiles[id] = &openEntry{
		localFd:        fd,
		dirty:          false,
		serverHandleID: serverHandleID,
		path:           path,
	}

	return id
}

func (cm *cacheManager) getOpenEntry(handleID int64) (*openEntry, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	entry, exists := cm.openFiles[handleID]
	return entry, exists

}

func (cm *cacheManager) markDirty(handleID int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if entry, exists := cm.openFiles[handleID]; exists {
		entry.dirty = true
	}
}

func (cm *cacheManager) closeFile(handleID int64) (*openEntry, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	entry, exists := cm.openFiles[handleID]

	if exists {
		delete(cm.openFiles, handleID)
	}
	return entry, exists

}
