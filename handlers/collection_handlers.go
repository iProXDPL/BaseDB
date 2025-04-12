package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"BaseDB/config"
	"BaseDB/models"
	"BaseDB/utils"
)

// handleCollectionOperation obsługuje operacje na kolekcjach
func handleCollectionOperation(w http.ResponseWriter, r *http.Request, dbName string, collName string, command string) {
	dbPath := utils.GetDatabasePath(config.DataDir, dbName)
	jsonFilePath := utils.GetCollectionPath(config.DataDir, dbName, collName)

	// Sprawdź czy baza danych istnieje
	if !utils.FileExists(dbPath) && command != "create" && command != "insertOne" && command != "insertMany" {
		http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
		return
	}

	switch command {
	case "create":
		createCollection(w, r, dbPath, jsonFilePath, dbName, collName)
	case "delete":
		deleteCollection(w, r, jsonFilePath, dbName, collName)
	case "rename":
		renameCollection(w, r, dbPath, jsonFilePath, dbName, collName)
	case "insertOne":
		insertOneDocument(w, r, dbPath, jsonFilePath, dbName, collName)
	case "insertMany":
		insertManyDocuments(w, r, dbPath, jsonFilePath, dbName, collName)
	case "updateOne":
		updateOneDocument(w, r, jsonFilePath, dbName, collName)
	case "updateMany":
		updateManyDocuments(w, r, jsonFilePath, dbName, collName)
	case "findOne":
		findOneDocument(w, r, jsonFilePath, dbName, collName)
	case "findMany":
		findManyDocuments(w, r, jsonFilePath, dbName, collName)
	case "find":
		find(w, r, jsonFilePath, dbName, collName)
	case "read":
		readCollection(w, r, jsonFilePath)
	default:
		http.Error(w, "Nieznana operacja w collections", http.StatusBadRequest)
	}
}

// createCollection tworzy nową kolekcję
func createCollection(w http.ResponseWriter, _ *http.Request, dbPath, jsonFilePath, dbName, collName string) {
	// Upewnij się, że katalog bazy danych istnieje
	if err := utils.EnsureDirectoryExists(dbPath); err != nil {
		http.Error(w, fmt.Sprintf("Nie można utworzyć katalogu bazy danych: %v", err), http.StatusInternalServerError)
		return
	}

	// Sprawdź czy kolekcja już istnieje
	if utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja już istnieje", http.StatusConflict)
		return
	}

	// Inicjalizuj pusty plik JSON jako tablicę
	emptyArray := []interface{}{}
	if err := utils.WriteJSONFile(jsonFilePath, emptyArray); err != nil {
		http.Error(w, fmt.Sprintf("Nie można utworzyć pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Kolekcja '%s' została utworzona w bazie '%s'", collName, dbName),
	})
}

// deleteCollection usuwa kolekcję
func deleteCollection(w http.ResponseWriter, _ *http.Request, jsonFilePath, dbName, collName string) {
	// Usuwanie pliku kolekcji
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	if err := os.Remove(jsonFilePath); err != nil {
		http.Error(w, fmt.Sprintf("Nie można usunąć kolekcji: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Kolekcja '%s' została usunięta z bazy '%s'", collName, dbName),
	})
}

// renameCollection zmienia nazwę kolekcji
func renameCollection(w http.ResponseWriter, r *http.Request, _, jsonFilePath, dbName, collName string) {
	// Zmiana nazwy pliku kolekcji
	newName := r.URL.Query().Get("newName")
	if newName == "" {
		http.Error(w, "Brak parametru 'newName'", http.StatusBadRequest)
		return
	}

	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	newJsonFilePath := utils.GetCollectionPath(config.DataDir, dbName, newName)

	if err := os.Rename(jsonFilePath, newJsonFilePath); err != nil {
		http.Error(w, fmt.Sprintf("Nie można zmienić nazwy kolekcji: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": fmt.Sprintf("Kolekcja '%s' została przemianowana na '%s'", collName, newName),
	})
}

// insertOneDocument dodaje jeden dokument do kolekcji
func insertOneDocument(w http.ResponseWriter, r *http.Request, dbPath, jsonFilePath, _, _ string) {
	// Sprawdź czy baza danych i kolekcja istnieją
	if !utils.FileExists(dbPath) {
		http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
		return
	}

	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje. Użyj 'createCollection' aby ją utworzyć", http.StatusNotFound)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Wymagana metoda POST", http.StatusMethodNotAllowed)
		return
	}

	var newData models.Document
	if err := json.NewDecoder(r.Body).Decode(&newData); err != nil {
		http.Error(w, fmt.Sprintf("Nieprawidłowy format JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Dodaj metadane (id, created_at, updated_at)
	newData = models.AddMetadata(newData)

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	// Jeśli plik nie istniał lub był pusty, zainicjuj pustą tablicę
	if data == nil {
		data = []models.Document{}
	}

	// Dodaj nowe dane
	data = append(data, newData)

	// Zapisz zaktualizowane dane
	if err := utils.WriteJSONFile(jsonFilePath, data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można zapisać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Dokument został dodany",
		"data":    newData,
	})
}

// insertManyDocuments dodaje wiele dokumentów do kolekcji
func insertManyDocuments(w http.ResponseWriter, r *http.Request, dbPath, jsonFilePath, _, _ string) {
	// Sprawdź czy baza danych i kolekcja istnieją
	if !utils.FileExists(dbPath) {
		http.Error(w, "Baza danych nie istnieje", http.StatusNotFound)
		return
	}

	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje. Użyj 'createCollection' aby ją utworzyć", http.StatusNotFound)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Wymagana metoda POST", http.StatusMethodNotAllowed)
		return
	}

	// Odczytaj tablicę dokumentów z żądania
	var newDocuments []models.Document
	if err := json.NewDecoder(r.Body).Decode(&newDocuments); err != nil {
		http.Error(w, fmt.Sprintf("Nieprawidłowy format JSON, oczekiwano tablicy dokumentów: %v", err), http.StatusBadRequest)
		return
	}

	// Dodaj metadane do każdego dokumentu
	for i := range newDocuments {
		newDocuments[i] = models.AddMetadata(newDocuments[i])
	}

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	// Jeśli plik nie istniał lub był pusty, zainicjuj pustą tablicę
	if data == nil {
		data = []models.Document{}
	}

	// Dodaj nowe dokumenty
	data = append(data, newDocuments...)

	// Zapisz zaktualizowane dane
	if err := utils.WriteJSONFile(jsonFilePath, data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można zapisać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "success",
		"message":        fmt.Sprintf("Dodano %d dokumentów", len(newDocuments)),
		"inserted_count": len(newDocuments),
		"documents":      newDocuments,
	})
}

// updateOneDocument aktualizuje jeden dokument w kolekcji
func updateOneDocument(w http.ResponseWriter, r *http.Request, jsonFilePath, _, _ string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	if r.Method != "PUT" && r.Method != "POST" {
		http.Error(w, "Wymagana metoda PUT lub POST", http.StatusMethodNotAllowed)
		return
	}

	// Odczytaj id dokumentu do aktualizacji
	documentID := r.URL.Query().Get("id")
	if documentID == "" {
		http.Error(w, "Brak parametru 'id'", http.StatusBadRequest)
		return
	}

	// Odczytaj dane aktualizacji
	var updateData models.Document
	if err := json.NewDecoder(r.Body).Decode(&updateData); err != nil {
		http.Error(w, fmt.Sprintf("Nieprawidłowy format JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if data == nil {
		http.Error(w, "Kolekcja jest pusta", http.StatusNotFound)
		return
	}

	// Znajdź dokument do aktualizacji
	documentFound := false
	for i, doc := range data {
		if id, ok := doc["id"]; ok && id == documentID {
			// Zachowaj oryginalne id i created_at
			updateData["id"] = documentID
			if createdAt, exists := doc["created_at"]; exists {
				updateData["created_at"] = createdAt
			}

			// Aktualizuj updated_at
			updateData["updated_at"] = models.GetCurrentTimestamp()

			// Aktualizuj dokument
			data[i] = updateData
			documentFound = true
			break
		}
	}

	if !documentFound {
		http.Error(w, fmt.Sprintf("Nie znaleziono dokumentu o id: %s", documentID), http.StatusNotFound)
		return
	}

	// Zapisz zaktualizowane dane
	if err := utils.WriteJSONFile(jsonFilePath, data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można zapisać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Dokument został zaktualizowany",
		"data":    updateData,
	})
}

// updateManyDocuments aktualizuje wiele dokumentów w kolekcji
func updateManyDocuments(w http.ResponseWriter, r *http.Request, jsonFilePath, _, _ string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	if r.Method != "PUT" && r.Method != "POST" {
		http.Error(w, "Wymagana metoda PUT lub POST", http.StatusMethodNotAllowed)
		return
	}

	// Odczytaj dane aktualizacji i zapytanie z ciała
	var requestBody struct {
		Query  map[string]interface{} `json:"query"`
		Update models.Document        `json:"update"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, fmt.Sprintf("Nieprawidłowy format JSON: %v", err), http.StatusBadRequest)
		return
	}

	if requestBody.Query == nil {
		http.Error(w, "Brak pola 'query' w żądaniu", http.StatusBadRequest)
		return
	}

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if data == nil {
		http.Error(w, "Kolekcja jest pusta", http.StatusNotFound)
		return
	}

	// Znajdź i zaktualizuj dokumenty
	updatedCount := 0
	updatedDocs := []models.Document{}

	for i, doc := range data {
		if matchesQuery(doc, requestBody.Query) {
			// Wykonaj aktualizację dokumentu
			updatedDoc := models.Document{}
			for k, v := range doc {
				updatedDoc[k] = v
			}

			// Aktualizuj pola
			for k, v := range requestBody.Update {
				updatedDoc[k] = v
			}

			// Zachowaj id i created_at
			updatedDoc["id"] = doc["id"]
			if createdAt, exists := doc["created_at"]; exists {
				updatedDoc["created_at"] = createdAt
			}

			// Aktualizuj updated_at
			updatedDoc["updated_at"] = models.GetCurrentTimestamp()

			// Zaktualizuj dokument w kolekcji
			data[i] = updatedDoc
			updatedCount++
			updatedDocs = append(updatedDocs, updatedDoc)
		}
	}

	if updatedCount == 0 {
		http.Error(w, "Nie znaleziono dokumentów spełniających kryteria", http.StatusNotFound)
		return
	}

	// Zapisz zaktualizowane dane
	if err := utils.WriteJSONFile(jsonFilePath, data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można zapisać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":        "success",
		"message":       fmt.Sprintf("Zaktualizowano %d dokumentów", updatedCount),
		"updated_count": updatedCount,
		"documents":     updatedDocs,
	})
}

// findOneDocument wyszukuje jeden dokument w kolekcji
func findOneDocument(w http.ResponseWriter, r *http.Request, jsonFilePath, _, _ string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	// Odczytaj kryteria wyszukiwania
	query := r.URL.Query()

	// Usuń 'command' z kryteriów wyszukiwania
	query.Del("command")

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if len(data) == 0 {
		http.Error(w, "Kolekcja jest pusta", http.StatusNotFound)
		return
	}

	// Wyszukaj dokument spełniający kryteria
	foundDocument := false
	var result models.Document

	for _, doc := range data {
		match := true
		for key, values := range query {
			// Sprawdź czy wartość dokumentu pasuje do wartości z zapytania
			if docValue, exists := doc[key]; exists {
				if len(values) > 0 && fmt.Sprintf("%v", docValue) != values[0] {
					match = false
					break
				}
			} else {
				match = false
				break
			}
		}

		if match {
			result = doc
			foundDocument = true
			break
		}
	}

	if !foundDocument {
		http.Error(w, "Nie znaleziono dokumentu spełniającego kryteria", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// findManyDocuments wyszukuje wiele dokumentów w kolekcji
func findManyDocuments(w http.ResponseWriter, r *http.Request, jsonFilePath, _, _ string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	// Odczytaj kryteria wyszukiwania
	query := r.URL.Query()

	// Pobierz i usuń parametry sortowania i paginacji z zapytania
	sortField := query.Get("sort")
	sortOrder := query.Get("order") // "asc" lub "desc"
	limit := query.Get("limit")
	skip := query.Get("skip")
	query.Del("command")
	query.Del("sort")
	query.Del("order")
	query.Del("limit")
	query.Del("skip")

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if data == nil {
		data = []models.Document{}
	}

	// Wyszukaj dokumenty spełniające kryteria
	var results []models.Document

	for _, doc := range data {
		match := true
		for key, values := range query {
			// Sprawdź czy wartość dokumentu pasuje do wartości z zapytania
			if docValue, exists := doc[key]; exists {
				if len(values) > 0 && fmt.Sprintf("%v", docValue) != values[0] {
					match = false
					break
				}
			} else {
				match = false
				break
			}
		}

		if match {
			results = append(results, doc)
		}
	}

	// Sortowanie wyników
	if sortField != "" {
		sortResults(results, sortField, sortOrder)
	}

	// Paginacja wyników
	skipCount := 0
	if skip != "" {
		skipCount, _ = strconv.Atoi(skip)
		if skipCount > len(results) {
			skipCount = len(results)
		}
	}

	limitCount := len(results)
	if limit != "" {
		parsed, err := strconv.Atoi(limit)
		if err == nil && parsed > 0 {
			limitCount = parsed
		}
	}

	// Zastosuj paginację
	end := skipCount + limitCount
	if end > len(results) {
		end = len(results)
	}

	if skipCount < len(results) {
		results = results[skipCount:end]
	} else {
		results = []models.Document{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"count":     len(results),
		"documents": results,
	})
}

// find wyszukuje wiele dokumentów w kolekcji z operatorami
func find(w http.ResponseWriter, r *http.Request, jsonFilePath, _, _ string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Kolekcja nie istnieje", http.StatusNotFound)
		return
	}

	// Odczytaj zapytanie z ciała lub parametrów URL
	var query map[string]interface{}

	if r.Method == "POST" {
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			http.Error(w, fmt.Sprintf("Nieprawidłowy format JSON: %v", err), http.StatusBadRequest)
			return
		}
	} else {
		// Twórz zapytanie na podstawie parametrów URL
		query = make(map[string]interface{})
		for k, v := range r.URL.Query() {
			if k != "command" && k != "sort" && k != "order" && k != "limit" && k != "skip" {
				if len(v) == 1 {
					query[k] = v[0]
				}
			}
		}
	}

	// Weryfikuj poprawność operatorów w zapytaniu
	if !validateOperators(w, query) {
		return // Error already written to response
	}

	// Pobierz parametry sortowania i paginacji
	urlQuery := r.URL.Query()
	sortField := urlQuery.Get("sort")
	sortOrder := urlQuery.Get("order") // "asc" lub "desc"
	limit := urlQuery.Get("limit")
	skip := urlQuery.Get("skip")

	// Odczytaj istniejące dane
	var data []models.Document
	if err := utils.ReadJSONFile(jsonFilePath, &data); err != nil {
		http.Error(w, fmt.Sprintf("Nie można odczytać pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}

	if data == nil {
		data = []models.Document{}
	}

	// Wyszukaj dokumenty spełniające kryteria
	var results []models.Document

	for _, doc := range data {
		if matchesQuery(doc, query) {
			results = append(results, doc)
		}
	}

	// Sortowanie wyników
	if sortField != "" {
		sortResults(results, sortField, sortOrder)
	}

	// Paginacja wyników
	skipCount := 0
	if skip != "" {
		skipCount, _ = strconv.Atoi(skip)
		if skipCount > len(results) {
			skipCount = len(results)
		}
	}

	limitCount := len(results)
	if limit != "" {
		parsed, err := strconv.Atoi(limit)
		if err == nil && parsed > 0 {
			limitCount = parsed
		}
	}

	// Zastosuj paginację
	end := skipCount + limitCount
	if end > len(results) {
		end = len(results)
	}

	if skipCount < len(results) {
		results = results[skipCount:end]
	} else {
		results = []models.Document{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "success",
		"count":     len(results),
		"documents": results,
	})
}

// readCollection odczytuje wszystkie dokumenty z kolekcji
func readCollection(w http.ResponseWriter, _ *http.Request, jsonFilePath string) {
	if !utils.FileExists(jsonFilePath) {
		http.Error(w, "Plik JSON nie istnieje", http.StatusNotFound)
		return
	}

	file, err := os.Open(jsonFilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Nie można otworzyć pliku JSON: %v", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/json")
	io.Copy(w, file)
}

////////////////////////////////

// matchesQuery sprawdza czy dokument spełnia kryteria zapytania z operatorami mongodb
func matchesQuery(doc models.Document, query map[string]interface{}) bool {
	for field, condition := range query {
		switch cond := condition.(type) {
		case map[string]interface{}:
			// Zapytanie zawiera operatory
			if !matchesOperators(doc, field, cond) {
				return false
			}
		default:
			// Proste zapytanie równości
			if docValue, exists := doc[field]; !exists || fmt.Sprintf("%v", docValue) != fmt.Sprintf("%v", condition) {
				return false
			}
		}
	}
	return true
}

// matchesOperators implementuje operatory MongoDB
func matchesOperators(doc models.Document, field string, operators map[string]interface{}) bool {
	docValue, exists := doc[field]
	if !exists {
		// Jeśli pole nie istnieje, zwróć false dla wszystkich operatorów poza $exists: false
		if existsOp, hasExistsOp := operators["$exists"]; hasExistsOp {
			if existsBool, ok := existsOp.(bool); ok && !existsBool {
				return true
			}
		}
		return false
	}

	for operator, value := range operators {
		switch operator {
		case "$eq":
			// Równość
			if fmt.Sprintf("%v", docValue) == fmt.Sprintf("%v", value) {
				return false
			}
		case "$ne":
			// Nierówność
			if fmt.Sprintf("%v", docValue) != fmt.Sprintf("%v", value) {
				return false
			}
		case "$gt":
			// Większe niż
			if !compareValues(docValue, value, func(a, b float64) bool { return a > b }) {
				return false
			}
		case "$gte":
			// Większe lub równe
			if !compareValues(docValue, value, func(a, b float64) bool { return a >= b }) {
				return false
			}
		case "$lt":
			// Mniejsze niż
			if !compareValues(docValue, value, func(a, b float64) bool { return a < b }) {
				return false
			}
		case "$lte":
			// Mniejsze lub równe
			if !compareValues(docValue, value, func(a, b float64) bool { return a <= b }) {
				return false
			}
		case "$in":
			// W zbiorze
			if !inArray(docValue, value) {
				return false
			}
		case "$nin":
			// Nie w zbiorze
			if inArray(docValue, value) {
				return false
			}
		case "$exists":
			// Istnieje
			if existsBool, ok := value.(bool); ok && existsBool != exists {
				return false
			}
		case "$regex":
			// Wyrażenie regularne
			if !matchesRegex(docValue, value) {
				return false
			}
		}
	}
	return true
}

// validateOperators checks for valid operators in the query and writes errors to HTTP response
func validateOperators(w http.ResponseWriter, query map[string]interface{}) bool {
	// Lista dozwolonych operatorów
	allowedOperators := map[string]bool{
		"$eq":     true,
		"$ne":     true,
		"$gt":     true,
		"$gte":    true,
		"$lt":     true,
		"$lte":    true,
		"$in":     true,
		"$nin":    true,
		"$exists": true,
		"$regex":  true,
	}

	// Sprawdź każde pole i jego operatory
	for field, condition := range query {
		// Jeśli wartość jest mapą, sprawdź operatory
		if condMap, ok := condition.(map[string]interface{}); ok {
			for op := range condMap {
				// Sprawdź czy operator rozpoczyna się od $
				if strings.HasPrefix(op, "$") {
					if !allowedOperators[op] {
						http.Error(w, fmt.Sprintf("Nieznany operator '%s' dla pola '%s'", op, field), http.StatusBadRequest)
						return false
					}

					// Sprawdź poprawność wartości dla danego operatora
					if !validateOperatorValue(w, op, condMap[op], field) {
						return false
					}
				}
			}

			// Sprawdź czy nie ma konfliktowych operatorów
			if condMap["$gt"] != nil && condMap["$lt"] != nil {
				gtVal, gtOk := toFloat64(condMap["$gt"])
				ltVal, ltOk := toFloat64(condMap["$lt"])
				if gtOk && ltOk && gtVal >= ltVal {
					http.Error(w, fmt.Sprintf("Konflikt operatorów dla pola '%s': $gt:%v musi być mniejsze niż $lt:%v", field, condMap["$gt"], condMap["$lt"]), http.StatusBadRequest)
					return false
				}
			}

			if condMap["$gte"] != nil && condMap["$lte"] != nil {
				gteVal, gteOk := toFloat64(condMap["$gte"])
				lteVal, lteOk := toFloat64(condMap["$lte"])
				if gteOk && lteOk && gteVal > lteVal {
					http.Error(w, fmt.Sprintf("Konflikt operatorów dla pola '%s': $gte:%v musi być mniejsze lub równe $lte:%v", field, condMap["$gte"], condMap["$lte"]), http.StatusBadRequest)
					return false
				}
			}
		}
	}

	return true
}

// validateOperatorValue sprawdza poprawność wartości dla danego operatora
func validateOperatorValue(w http.ResponseWriter, operator string, value interface{}, field string) bool {
	switch operator {
	case "$in", "$nin":
		// Sprawdź czy wartość jest tablicą
		_, ok := value.([]interface{})
		if !ok {
			http.Error(w, fmt.Sprintf("Operator %s wymaga tablicy wartości dla pola '%s'", operator, field), http.StatusBadRequest)
			return false
		}
	case "$exists":
		// Sprawdź czy wartość jest typu bool
		_, ok := value.(bool)
		if !ok {
			http.Error(w, fmt.Sprintf("Operator %s wymaga wartości logicznej (true/false) dla pola '%s'", operator, field), http.StatusBadRequest)
			return false
		}
	case "$regex":
		// Sprawdź czy wartość jest poprawnym wyrażeniem regularnym
		regexStr, ok := value.(string)
		if !ok {
			http.Error(w, fmt.Sprintf("Operator %s wymaga wartości typu string dla pola '%s'", operator, field), http.StatusBadRequest)
			return false
		}
		if _, err := regexp.Compile(regexStr); err != nil {
			http.Error(w, fmt.Sprintf("Nieprawidłowe wyrażenie regularne dla operatora %s w polu '%s': %v", operator, field, err), http.StatusBadRequest)
			return false
		}
	case "$gt", "$gte", "$lt", "$lte":
		// Dla operatorów porównania, nie ma konkretnego wymagania co do typu
		// ale warto sprawdzić czy nie są to wartości złożone
		switch value.(type) {
		case map[string]interface{}, []interface{}:
			http.Error(w, fmt.Sprintf("Operator %s nie może przyjmować wartości złożonych (obiekty, tablice) dla pola '%s'", operator, field), http.StatusBadRequest)
			return false
		}
	}
	return true
}

// compareValues porównuje wartości dla operatorów porównania
func compareValues(docValue, queryValue interface{}, compareFunc func(a, b float64) bool) bool {
	// Konwertuj do float64 dla porównania liczbowego
	docFloat, docOk := toFloat64(docValue)
	queryFloat, queryOk := toFloat64(queryValue)

	if docOk && queryOk {
		return compareFunc(docFloat, queryFloat)
	}

	// Dla wartości nieliczbowych, porównaj jako stringi
	docStr := fmt.Sprintf("%v", docValue)
	queryStr := fmt.Sprintf("%v", queryValue)
	return compareFunc(float64(strings.Compare(docStr, queryStr)), 0)
}

// toFloat64 konwertuje wartość do float64
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// inArray sprawdza czy wartość jest w tablicy
func inArray(docValue, queryValue interface{}) bool {
	queryArray, ok := queryValue.([]interface{})
	if !ok {
		return false
	}

	for _, item := range queryArray {
		if fmt.Sprintf("%v", docValue) == fmt.Sprintf("%v", item) {
			return true
		}
	}
	return false
}

// matchesRegex sprawdza czy wartość pasuje do wyrażenia regularnego
func matchesRegex(docValue, queryValue interface{}) bool {
	docStr := fmt.Sprintf("%v", docValue)
	regexStr, ok := queryValue.(string)
	if !ok {
		return false
	}

	regex, err := regexp.Compile(regexStr)
	if err != nil {
		return false
	}

	return regex.MatchString(docStr)
}

// sortResults sortuje dokumenty według podanego pola
func sortResults(docs []models.Document, field, order string) {
	sort.Slice(docs, func(i, j int) bool {
		// Pobierz wartości pola
		var vi, vj interface{}
		var existi, existj bool

		if vi, existi = docs[i][field]; !existi {
			return false
		}

		if vj, existj = docs[j][field]; !existj {
			return true
		}

		// Sprawdź, czy pole może być typu czasowego (created_at, updated_at)
		if isTimeField(field) || isTimeValue(vi) || isTimeValue(vj) {
			// Spróbuj skonwertować do czasu
			ti, oki := parseTime(vi)
			tj, okj := parseTime(vj)

			if oki && okj {
				if order == "desc" {
					return ti.After(tj)
				}
				return ti.Before(tj)
			}
		}

		// Porównaj wartości w zależności od typu
		switch vi.(type) {
		case string:
			// Porównanie stringów
			si, oki := vi.(string)
			sj, okj := vj.(string)
			if oki && okj {
				if order == "desc" {
					return si > sj
				}
				return si < sj
			}
		case float64:
			// Porównanie liczb
			fi, oki := vi.(float64)
			fj, okj := vj.(float64)
			if oki && okj {
				if order == "desc" {
					return fi > fj
				}
				return fi < fj
			}
		case bool:
			// Porównanie wartości logicznych
			bi, oki := vi.(bool)
			bj, okj := vj.(bool)
			if oki && okj {
				if order == "desc" {
					return !bi && bj
				}
				return bi && !bj
			}
		}

		// Domyślne porównanie stringów
		si := fmt.Sprintf("%v", vi)
		sj := fmt.Sprintf("%v", vj)
		if order == "desc" {
			return si > sj
		}
		return si < sj
	})
}

// isTimeField sprawdza czy nazwa pola wskazuje na pole czasowe
func isTimeField(field string) bool {
	timeFields := []string{"created_at", "updated_at", "timestamp", "date", "time"}
	for _, tf := range timeFields {
		if field == tf || strings.HasSuffix(field, "_"+tf) {
			return true
		}
	}
	return false
}

// isTimeValue sprawdza czy wartość wygląda jak reprezentacja czasu
func isTimeValue(value interface{}) bool {
	// Sprawdź czy to liczba (timestamp)
	if _, ok := value.(float64); ok {
		return true
	}

	// Sprawdź czy to string i czy pasuje do formatów czasu
	if strVal, ok := value.(string); ok {
		// Sprawdź czy to format ISO8601 lub podobny
		isoPattern := `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$`
		isoRegex := regexp.MustCompile(isoPattern)
		if isoRegex.MatchString(strVal) {
			return true
		}

		// Sprawdź inne typowe formaty daty
		datePatterns := []string{
			`^\d{4}-\d{2}-\d{2}$`,                   // YYYY-MM-DD
			`^\d{2}/\d{2}/\d{4}$`,                   // DD/MM/YYYY lub MM/DD/YYYY
			`^\d{2}\.\d{2}\.\d{4}$`,                 // DD.MM.YYYY
			`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`, // YYYY-MM-DD HH:MM:SS
		}

		for _, pattern := range datePatterns {
			r := regexp.MustCompile(pattern)
			if r.MatchString(strVal) {
				return true
			}
		}
	}

	return false
}

// parseTime próbuje sparsować wartość jako czas
func parseTime(value interface{}) (time.Time, bool) {
	// Jeśli to float64, potraktuj jako timestamp Unix
	if ts, ok := value.(float64); ok {
		return time.Unix(int64(ts), 0), true
	}

	// Jeśli to string, sprawdź różne formaty
	if strVal, ok := value.(string); ok {
		// Listę formatów do sprawdzenia
		formats := []string{
			time.RFC3339,
			"2006-01-02T15:04:05Z",
			"2006-01-02",
			"02/01/2006",
			"01/02/2006",
			"02.01.2006",
			"2006-01-02 15:04:05",
			// Dodaj inne formaty według potrzeb
		}

		for _, format := range formats {
			if t, err := time.Parse(format, strVal); err == nil {
				return t, true
			}
		}

		// Sprawdź czy to timestamp jako string
		if i, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			return time.Unix(i, 0), true
		}
	}

	return time.Time{}, false
}
