package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"BaseDB/config"
	"BaseDB/utils"
)

// handleDatabaseOperation obsługuje operacje na bazach danych
func handleDatabaseOperation(w http.ResponseWriter, r *http.Request, dbName string, command string) {
	dbPath := utils.GetDatabasePath(config.DataDir, dbName)

	switch command {
	case "create":
		// Tworzenie katalogu bazy danych
		if err := utils.EnsureDirectoryExists(dbPath); err != nil {
			http.Error(w, fmt.Sprintf("Nie można utworzyć bazy danych: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": fmt.Sprintf("Baza danych '%s' została utworzona", dbName),
		})

	case "delete":
		// Usuwanie katalogu bazy danych
		if !utils.FileExists(dbPath) {
			http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
			return
		}

		if err := os.RemoveAll(dbPath); err != nil {
			http.Error(w, fmt.Sprintf("Nie można usunąć bazy danych: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": fmt.Sprintf("Baza danych '%s' została usunięta", dbName),
		})

	case "rename":
		// Zmiana nazwy bazy danych
		newName := r.URL.Query().Get("newName")
		if newName == "" {
			http.Error(w, "Brak parametru 'newName'", http.StatusBadRequest)
			return
		}

		newPath := utils.GetDatabasePath(config.DataDir, newName)

		if !utils.FileExists(dbPath) {
			http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
			return
		}

		if err := os.Rename(dbPath, newPath); err != nil {
			http.Error(w, fmt.Sprintf("Nie można zmienić nazwy bazy danych: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": fmt.Sprintf("Baza danych '%s' została przemianowana na '%s'", dbName, newName),
		})

	case "list":
		// Domyślnie listuje kolekcje w bazie danych
		if !utils.FileExists(dbPath) {
			http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
			return
		}

		collections, err := utils.ListJSONFiles(dbPath)
		if err != nil {
			http.Error(w, fmt.Sprintf("Błąd odczytu katalogu: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":      "success",
			"database":    dbName,
			"collections": collections,
		})
	default:
		http.Error(w, "Nieznana operacja w database", http.StatusBadRequest)
	}
}
