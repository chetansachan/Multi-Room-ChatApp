package main

import (
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Room struct {
	roomname string
	clients  map[string]*Client
	mu       sync.RWMutex
}

type RoomManager struct {
	rooms map[string]*Room // {
	// "afdadsjf13241" : {roomname: "afdadsjf13241", clent : Client},
	// "afdadsjf132414213" : {roomname: "afdadsjf132414213", clent : Client}
	// }
	mu sync.RWMutex
}

type Client struct {
	id   uuid.UUID
	conn *websocket.Conn
	send chan []byte
}

type Message struct {
	sender  *Client
	content []byte
}

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan *Message
}

func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*Room),
	}
}

func (rm *RoomManager) GetorCreateRoom(roomname string) *Room {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	//check if the room exist return the room
	if room, ok := rm.rooms[roomname]; ok {
		return room
	}

	//create empty room with this room name
	room := &Room{
		clients: map[string]*Client{},
		/*

			clients : {
				"aafjdslkjasldk1324134134" : "x000fff",
				"aafjdslkjasldk1324134134" : "x000fff",
				"aafjdslkjasldk1324134134" : "x000fff",
				"aafjdslkjasldk1324134134" : "x000fff",
				"aafjdslkjasldk1324134134" : "x000fff",
			}

		*/
		roomname: roomname,
	}

	rm.rooms[roomname] = room
	return room
}

func (rm *RoomManager) JoinRoom(roomname string, client *Client) {
	//get room from GetorCreateRoom with this roomname
	room := rm.GetorCreateRoom(roomname)

	rm.mu.Lock()
	room.clients[client.id.String()] = client
	rm.mu.RUnlock()

	log.Print("Client Joined Room")
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *Message),
	}
}

func (c *Client) readPump(h *Hub) {
	defer func() {
		h.unregister <- c
		c.conn.Close()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Print("Unable to read message")
			}
			break
		}
		h.broadcast <- &Message{
			sender:  c,
			content: message,
		}
	}
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()

	for message := range c.send {
		err := c.conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Print("Unable to write message")
			return
		}
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Print("Client Connected")

		case client := <-h.unregister:
			delete(h.clients, client)
			close(client.send)
			log.Print("Client Disconnected")

		case message := <-h.broadcast:
			for client := range h.clients {
				if client == message.sender {
					continue
				}
				client.send <- message.content
			}
		}
	}
}

func serverWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("Error Upgraging Connection")
	}

	client := &Client{
		id:   uuid.New(),
		conn: conn,
		send: make(chan []byte, 256),
	}
	hub.register <- client
	go client.readPump(hub)
	go client.writePump()
}

func main() {
	hub := NewHub()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serverWs(hub, w, r)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	log.Print("Server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Print("Error running server")
	}

}
