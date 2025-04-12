package utils

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// EnsureDirectoryExists upewnia się, że podany katalog istnieje
func EnsureDirectoryExists(path string) error {
	return os.MkdirAll(path, 0755)
}

// FileExists sprawdza czy plik istnieje
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// ReadJSONFile odczytuje plik JSON i dekoduje go do podanej struktury
func ReadJSONFile(path string, v interface{}) error {
	// Jeśli plik nie istnieje, zwróć puste dane
	if !FileExists(path) {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Jeśli plik jest pusty, zwróć nil
	if len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, v)
}

// WriteJSONFile zapisuje strukturę do pliku JSON
func WriteJSONFile(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// ListJSONFiles zwraca listę plików JSON w katalogu
func ListJSONFiles(dirPath string) ([]string, error) {
	// Sprawdź czy katalog istnieje
	if !FileExists(dirPath) {
		return []string{}, nil
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Usuń rozszerzenie .json
			name := strings.TrimSuffix(entry.Name(), ".json")
			files = append(files, name)
		}
	}

	return files, nil
}

// GetDatabasePath zwraca pełną ścieżkę do katalogu bazy danych
func GetDatabasePath(baseDir, dbName string) string {
	return filepath.Join(baseDir, dbName)
}

// GetCollectionPath zwraca pełną ścieżkę do pliku kolekcji
func GetCollectionPath(baseDir, dbName, collName string) string {
	return filepath.Join(baseDir, dbName, collName+".json")
}
