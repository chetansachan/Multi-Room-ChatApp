package main

import (
	"encoding/json"
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
	Type    string          `json:"type"`
	Room    string          `json:"room,omitempty"`
	Content json.RawMessage `json:"content,omitempty"`
}

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
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

	room.mu.Lock()
	room.clients[client.id.String()] = client
	room.mu.Unlock()

	log.Print("Client Joined Room")
}

func (rm *RoomManager) LeaveRoom(roomname string, client *Client) {
	rm.mu.RLock()
	//check if their if room with this name if not return room not exist
	room, exist := rm.rooms[roomname]
	rm.mu.RUnlock()

	if !exist {
		log.Print("Room not exist")
		return
	}

	room.mu.Lock()
	//delete client from rooom client map by using client id
	delete(room.clients, client.id.String())

	//if their is no client in the room delete the room
	clientcount := len(room.clients)
	room.mu.Unlock()

	log.Print("Client leaved the room")

	if clientcount == 0 {
		rm.mu.Lock()
		room.mu.Lock()

		deleted := false
		if current, ok := rm.rooms[roomname]; ok && current == room && len(room.clients) == 0 {
			delete(rm.rooms, roomname)
			deleted = true
		}
		room.mu.Unlock()
		rm.mu.Unlock()

		if deleted {
			log.Print("Empty room is deleted")
		}
	}

}

func (rm *RoomManager) IsMember(roomname, clientID string) bool {
	rm.mu.RLock()
	room, exist := rm.rooms[roomname]
	rm.mu.RUnlock()
	if !exist {
		return false
	}
 
	room.mu.RLock()
	defer room.mu.RUnlock()
	_, ok := room.clients[clientID]
	return ok
}

func (rm *RoomManager) Broadcast(roomname string, message []byte, senderId string) {
	rm.mu.RLock()
	//check if their is room with this name if not return room not exist
	room, exist := rm.rooms[roomname]
	rm.mu.RUnlock()

	if !exist {
		log.Print("Room not exist")
		return
	}

	room.mu.RLock()
	defer room.mu.RUnlock()
	for id, client := range room.clients {
		if id == senderId {
			continue
		}
		select {
		case client.send <- message:
		default:
			log.Printf("Room broadcast: client %s buffer full", id)
		}
	}

}

func HandleMessage(client *Client, rm *RoomManager, data []byte) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("Invalid message from %s: %v", client.id, err)
		return
	}
	switch msg.Type {
	case "join":
		rm.JoinRoom(msg.Room, client)
		res := Message{Type: "joined", Room: msg.Room}
		if output, err := json.Marshal(res); err == nil {
			client.send <- output
		}

	case "leave":
		rm.LeaveRoom(msg.Room, client)
		res := Message{Type: "left", Room: msg.Room}
		if output, err := json.Marshal(res); err == nil {
			client.send <- output
		}

	case "message":
		if !rm.IsMember(msg.Room, client.id.String()) {
			log.Printf("Client %s tried to send to room %s without being a member", client.id, msg.Room)
			res := Message{
				Type:    "error",
				Room:    msg.Room,
				Content: json.RawMessage(`"you are not a member of this room"`),
			}
			if output, err := json.Marshal(res); err == nil {
				client.send <- output
			}
			return
		}
		rm.Broadcast(msg.Room, data, client.id.String())
	}
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (c *Client) readPump(h *Hub, rm *RoomManager) {
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
		HandleMessage(c, rm, message)
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
		}
	}
}

func serverWs(hub *Hub, rm *RoomManager, w http.ResponseWriter, r *http.Request) {
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
	go client.readPump(hub, rm)
	go client.writePump()
}

func main() {
	hub := NewHub()
	rm := NewRoomManager()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serverWs(hub, rm, w, r)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	log.Print("Server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Print("Error running server")
	}

}
