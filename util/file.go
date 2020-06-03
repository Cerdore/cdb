package util

import (
	"fmt"
	"os"
	"path"
)

// CreateFile is a helper for WAL, SSTable, Manifest and other classes that need to create files
// for on disk output
func CreateFile(filename string, dbName string, DataDir string) (*os.File, error) {
	tablePath := path.Join(DataDir, dbName, filename)
	if _, err := os.Stat(tablePath); !os.IsNotExist(err) { //存在
		if err != nil {
			return nil, fmt.Errorf("failure checking for %s existence: %w", tablePath, err)
		} else {
			return nil, fmt.Errorf("attempting to create %s but already exists", tablePath)
		}
	}

	file, err := os.Create(tablePath)
	if err != nil {
		return nil, fmt.Errorf("could not create %s file: %w", tablePath, err)
	}

	return file, nil
}
