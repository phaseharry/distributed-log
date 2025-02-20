package main

import (
	"log"

	"github.com/phaseharry/distributed-services-with-go/http-server/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
