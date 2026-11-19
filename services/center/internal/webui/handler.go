// Package webui serves the built web UI from [go.ui].static_dir.
package webui

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// CheckDir accepts "" (no UI) or a directory holding a regular index.html. The API calls it at
// startup, before it opens the database, so a typo or an image built without the UI exits
// instead of serving 404s.
func CheckDir(dir string) error {
	if dir == "" {
		return nil
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("%q must be an existing directory: %v", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%q must be a directory", dir)
	}
	index, err := os.Stat(filepath.Join(dir, "index.html"))
	if err != nil {
		return fmt.Errorf("%q must contain index.html: %v", dir, err)
	}
	if !index.Mode().IsRegular() {
		return fmt.Errorf("%q: index.html must be a regular file", dir)
	}
	return nil
}

// Handler serves the files in dir. It is mounted at "/" for every method (a "GET /" pattern
// would overlap "/upload/" and make ServeMux panic), so it answers 405 itself for anything but
// GET and HEAD. A directory without index.html and any path segment starting with "." are 404,
// so the file tree is never listed. Symlinks inside dir are followed: it is our own build output.
func Handler(dir string) http.Handler {
	files := http.FileServer(siteFS{http.Dir(dir)})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// A path ending in "/" is served from its index.html. Revalidating it on every load
		// makes a rebuilt UI show after a reload; assets keep the FileServer defaults.
		if strings.HasSuffix(r.URL.Path, "/") {
			w.Header().Set("Cache-Control", "no-cache")
		}
		files.ServeHTTP(w, r)
	})
}

// siteFS hides dotfiles and directories that have no index.html.
type siteFS struct {
	root http.FileSystem
}

func (s siteFS) Open(name string) (http.File, error) {
	for _, segment := range strings.Split(name, "/") {
		if strings.HasPrefix(segment, ".") {
			return nil, fs.ErrNotExist
		}
	}
	f, err := s.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if info.IsDir() {
		index, err := s.root.Open(strings.TrimSuffix(name, "/") + "/index.html")
		if err != nil {
			f.Close()
			return nil, fs.ErrNotExist
		}
		index.Close()
	}
	return f, nil
}
