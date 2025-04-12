package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"BaseDB/config"
)

// HandleAPI obsługuje wszystkie żądania do API
func HandleAPI(w http.ResponseWriter, r *http.Request) {
	// Parsowanie ścieżki i parametrów
	path := strings.TrimPrefix(r.URL.Path, "/api/database/")
	segments := strings.Split(path, "/")
	command := r.URL.Query().Get("command")

	// Obsługa różnych poziomów ścieżki
	switch len(segments) {
	case 1: // /api/{nameDB}?command=...
		if segments[0] == "" {
			listDatabases(w, r)
			return
		}
		handleDatabaseOperation(w, r, segments[0], command)
	case 2: // /api/{nameDB}/{nameCollection}?command=...
		handleCollectionOperation(w, r, segments[0], segments[1], command)
	default:
		http.Error(w, "Nieprawidłowa ścieżka", http.StatusBadRequest)
	}
}

// listDatabases wyświetla listę wszystkich baz danych
func listDatabases(w http.ResponseWriter, _ *http.Request) {
	entries, err := os.ReadDir(config.DataDir)
	if err != nil {
		http.Error(w, fmt.Sprintf("Błąd odczytu katalogu: %v", err), http.StatusInternalServerError)
		return
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"databases": databases,
	})
}
