package models

import (
	"time"

	"github.com/google/uuid"
)

// Document reprezentuje dokument w kolekcji
type Document map[string]interface{}

// AddMetadata dodaje metadane do dokumentu (id, created_at, updated_at)
func AddMetadata(doc Document) Document {
	// Jeśli dokument już ma id, zachowaj je
	if _, exists := doc["id"]; !exists {
		doc["id"] = uuid.New().String()
	}

	now := GetCurrentTimestamp()

	// Dodaj created_at tylko jeśli nie istnieje
	if _, exists := doc["created_at"]; !exists {
		doc["created_at"] = now
	}

	// Zawsze aktualizuj updated_at
	doc["updated_at"] = now

	return doc
}

// GetCurrentTimestamp zwraca aktualny czas w formacie RFC3339
func GetCurrentTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
