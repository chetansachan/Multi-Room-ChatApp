package main

import (
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	id uuid.UUID
	conn *websocket.Conn
	send chan []byte
}

func main() {

}