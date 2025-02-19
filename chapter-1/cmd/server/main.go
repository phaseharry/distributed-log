package main

import (
	"log"

	"github.com/phaseharry/distributed-services-with-go/chapter-1/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
