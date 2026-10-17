// Package store holds in-memory data stores for the superpeer

package store

import (
	"errors"
	"path"
	"strings"
	"sync"
)

const (
	NodeTypeFile      = "file"
	NodeTypeDirectory = "directory"
)

var (
	ErrPathNotFound     = errors.New("path not found")
	ErrPathAlreadyExists = errors.New("path already exists")
	ErrNotADirectory    = errors.New("not a directory")
)

// FsNode represents a file or directory in the logical filesystem.
type FsNode struct {
	Name     string
	Type     string
	FileID   string             // Populated only if Type == NodeTypeFile
	Children map[string]*FsNode // Populated only if Type == NodeTypeDirectory
}

// FsStore manages an isolated filesystem tree per user
type FsStore struct {
	mu    sync.RWMutex
	roots map[string]*FsNode // userID - root directory
}

// NewFsStore creates a new FsStore
func NewFsStore() *FsStore {
	return &FsStore{
		roots: make(map[string]*FsNode),
	}
}

// getRoot returns the root directory for a user, creating it if necessary
func (s *FsStore) getRoot(userID string) *FsNode {
	if root, exists := s.roots[userID]; exists {
		return root
	}
	root := &FsNode{
		Name:     "/",
		Type:     NodeTypeDirectory,
		Children: make(map[string]*FsNode),
	}
	s.roots[userID] = root
	return root
}

// traverse walks the tree following the path components
// Returns the parent node and the target child's name, or an error
func (s *FsStore) traverse(userID string, logicalPath string) (parent *FsNode, childName string, err error) {
	cleaned := path.Clean(logicalPath)
	if cleaned == "/" || cleaned == "." {
		return nil, "", errors.New("cannot modify root directory")
	}

	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	childName = parts[len(parts)-1]
	parentParts := parts[:len(parts)-1]

	current := s.getRoot(userID)
	for _, part := range parentParts {
		child, exists := current.Children[part]
		if !exists {
			return nil, "", ErrPathNotFound
		}
		if child.Type != NodeTypeDirectory {
			return nil, "", ErrNotADirectory
		}
		current = child
	}

	return current, childName, nil
}

// Mkdir creates a new directory. Fails if the parent doesn't exist
func (s *FsStore) Mkdir(userID, logicalPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parent, childName, err := s.traverse(userID, logicalPath)
	if err != nil {
		return err
	}

	if _, exists := parent.Children[childName]; exists {
		return ErrPathAlreadyExists
	}

	parent.Children[childName] = &FsNode{
		Name:     childName,
		Type:     NodeTypeDirectory,
		Children: make(map[string]*FsNode),
	}
	return nil
}

// Rmdir removes a directory and all its contents recursively
// It returns the file_id of every file that was inside, so the caller can delete their data too
func (s *FsStore) Rmdir(userID, logicalPath string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parent, childName, err := s.traverse(userID, logicalPath)
	if err != nil {
		return nil, err
	}

	target, exists := parent.Children[childName]
	if !exists {
		return nil, ErrPathNotFound
	}
	if target.Type != NodeTypeDirectory {
		return nil, ErrNotADirectory
	}

	delete(parent.Children, childName)
	return collectFileIDs(target), nil
}

// collectFileIDs returns the file_id of every file in the subtree of node
func collectFileIDs(node *FsNode) []string {
	if node.Type == NodeTypeFile {
		return []string{node.FileID}
	}

	var fileIDs []string
	for _, child := range node.Children {
		fileIDs = append(fileIDs, collectFileIDs(child)...)
	}
	return fileIDs
}

// Ls returns the children of a given directory
func (s *FsStore) Ls(userID, logicalPath string) ([]*FsNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cleaned := path.Clean(logicalPath)
	var target *FsNode

	if cleaned == "/" || cleaned == "." {
		target = s.getRoot(userID)
	} else {
		parent, childName, err := s.traverse(userID, logicalPath)
		if err != nil {
			return nil, err
		}
		var exists bool
		target, exists = parent.Children[childName]
		if !exists {
			return nil, ErrPathNotFound
		}
	}

	if target.Type != NodeTypeDirectory {
		return nil, ErrNotADirectory
	}

	var children []*FsNode
	for _, child := range target.Children {
		// Return shallow copies to avoid external mutation
		c := *child
		c.Children = nil // Avoid leaking the internal map
		children = append(children, &c)
	}

	return children, nil
}


// RemoveFile removes the file node at logicalPath and returns its file_id
// Fails with ErrPathNotFound if the path does not exist or is not a file
func (s *FsStore) RemoveFile(userID, logicalPath string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parent, childName, err := s.traverse(userID, logicalPath)
	if err != nil {
		return "", err
	}

	target, exists := parent.Children[childName]
	if !exists || target.Type != NodeTypeFile {
		return "", ErrPathNotFound
	}

	delete(parent.Children, childName)
	return target.FileID, nil
}

// Lookup returns the file_id of the file at logicalPath
// Fails with ErrPathNotFound if the path does not exist or is not a file
func (s *FsStore) Lookup(userID, logicalPath string) (string, error) {
	// Write lock because traverse may create the user's root directory
	s.mu.Lock()
	defer s.mu.Unlock()

	parent, childName, err := s.traverse(userID, logicalPath)
	if err != nil {
		return "", err
	}

	target, exists := parent.Children[childName]
	if !exists || target.Type != NodeTypeFile {
		return "", ErrPathNotFound
	}
	return target.FileID, nil
}

// CreateFile creates a new file node in the filesystem
// Fails if the parent doesn't exist or if the path already exists
func (s *FsStore) CreateFile(userID, logicalPath, fileID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parent, childName, err := s.traverse(userID, logicalPath)
	if err != nil {
		return err
	}

	if _, exists := parent.Children[childName]; exists {
		return ErrPathAlreadyExists
	}

	parent.Children[childName] = &FsNode{
		Name:   childName,
		Type:   NodeTypeFile,
		FileID: fileID,
	}
	return nil
}
