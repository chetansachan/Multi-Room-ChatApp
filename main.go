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

type Hub struct {
	clients map[*Client] bool
	register chan *Client
	unregister chan *Client
	broadcast chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[*Client]bool),
		register: make(chan *Client),
		unregister: make(chan *Client),
		broadcast: make(chan []byte),
	}
}

func main() {

}