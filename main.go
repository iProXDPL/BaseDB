// main.go
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"BaseDB/config"
	"BaseDB/handlers"
)

func main() {
	// Upewnij się, że katalog danych istnieje
	os.MkdirAll(config.DataDir, 0755)

	// Definicja tras
	http.HandleFunc("/api/database/", handlers.HandleAPI)

	// Uruchomienie serwera
	fmt.Printf("Serwer uruchomiony na http://localhost:%s\n", config.Port)
	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}
