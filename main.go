package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// creating upgrader variable to use it to upgrage our http connection to websocket connection
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// creating room struct with fields roomname which will have type string, clients which will have type map with key string and value client address and mu which will have type rwmutex
type Room struct {
	roomname string
	clients  map[string]*Client
	mu       sync.RWMutex
}

// creating RoomManager struct with fields rooms which will have type map with key string and value room address and mu which will have type rwmutex
type RoomManager struct {
	rooms map[string]*Room
	mu    sync.RWMutex
}

// creating Client struct with fields id which will have type uuid, conn which will have type websocket.Conn address and send which will have type array of byte channel
type Client struct {
	id   uuid.UUID
	conn *websocket.Conn
	send chan []byte
}

// creating Message struct with fields type which will have type string, Room which will have type string, Content which will have type json.RawMessage and their json format fields
type Message struct {
	Type    string          `json:"type"`
	Room    string          `json:"room,omitempty"`
	Content json.RawMessage `json:"content,omitempty"`
}

// creating Hub struct with fields clients which will have type map with key client address and value boolean, register which will have type channel of client address, unregister which will have type channel of client address
type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
}

// creating NewRoomManager func which will initializes and returns empty rooms map
func NewRoomManager() *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*Room),
	}
}

// creating NewHub func which will initializes and returns empty clients map and make channel
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// creating GetorCreateRoom which will return existing romm or create new room and returns it
func (rm *RoomManager) GetorCreateRoom(roomname string) *Room {
	//lock roommanager mutex and unlock when func ends
	rm.mu.Lock()
	defer rm.mu.Unlock()

	//check from the key in the rooms map that is roomname exist and it will return room which we store in room variable and make ok true and when ok true return room
	if room, ok := rm.rooms[roomname]; ok {
		return room
	}

	//and if room doesnt exist of that name then create a empty room with this room name and empty clients map for client to join later
	room := &Room{
		clients:  map[string]*Client{},
		roomname: roomname,
	}

	//add value of room at key roomname in rooms map and return the room that is created
	rm.rooms[roomname] = room
	return room
}

// creating JoinRoom func to join the room
func (rm *RoomManager) JoinRoom(roomname string, client *Client) {
	//we will send roomname in GetorCreateRoom func and it will check if room exist of that name if not then create room or give existing room for us and store that in room variable
	room := rm.GetorCreateRoom(roomname)

	//lock room mutex
	room.mu.Lock()
	//add value client at key client id in clients map
	room.clients[client.id.String()] = client
	//unlock room mutex
	room.mu.Unlock()

	//print Client Joined Room
	log.Print("Client Joined Room")
}

// creating LeaveRoom func to leave the room
func (rm *RoomManager) LeaveRoom(roomname string, client *Client) {
	//lock RoomManager mutex
	rm.mu.RLock()
	//check if their is room with this key roomname in rooms map make exist true and store the room in room variable and if exist not true return room not exist
	room, exist := rm.rooms[roomname]
	//unlock RoomManager mutex
	rm.mu.RUnlock()

	if !exist {
		log.Print("Room not exist")
		return
	}

	//lock room mutex
	room.mu.Lock()
	//delete client at key client id in clients map
	delete(room.clients, client.id.String())

	//stor length of clients map in clientcount variable
	clientcount := len(room.clients)
	//lock room mutex
	room.mu.Unlock()

	//print Client leaved the room
	log.Print("Client leaved the room")

	//check if clientcount is equal to zero
	if clientcount == 0 {
		//lock RoomManager mutex and room mutex
		rm.mu.Lock()
		room.mu.Lock()

		//at first deleted is false
		deleted := false
		//if there is room of key roomname in rooms map make ok true and store room in current variable and if true and current is equal to room and length of clients map is equal to zero then delete room at key roomname in rooms map and make deleted true
		if current, ok := rm.rooms[roomname]; ok && current == room && len(room.clients) == 0 {
			delete(rm.rooms, roomname)
			deleted = true
		}
		//Unlock RoomManager mutex and room mutex
		room.mu.Unlock()
		rm.mu.Unlock()

		//if deleted true print Empty room is deleted
		if deleted {
			log.Print("Empty room is deleted")
		}
	}

}

// create IsMember to check if client is member of a room or not
func (rm *RoomManager) IsMember(roomname, clientID string) bool {
	//lock RoomManager mutex
	rm.mu.RLock()
	//find room of key roomname in rooms map and store it in room variable and make exist true and if exist is not true then return false
	room, exist := rm.rooms[roomname]
	//Unlock RoomManager mutex
	rm.mu.RUnlock()
	if !exist {
		return false
	}

	//lock room mutex and unlock when func ends
	room.mu.RLock()
	defer room.mu.RUnlock()
	//find client of key clientID in clients map and make ok true and return ok
	_, ok := room.clients[clientID]
	return ok
}

// creating broadcast func to broadcast message to all others client in a room
func (rm *RoomManager) Broadcast(roomname string, message []byte, senderId string) {
	//lock RoomManager mutex
	rm.mu.RLock()
	//check if their is room with this key roomname in rooms map make exist true and store the room in room variable and if exist not true return room not exist
	room, exist := rm.rooms[roomname]
	//Unlock RoomManager mutex
	rm.mu.RUnlock()

	if !exist {
		log.Print("Room not exist")
		return
	}

	//lock room mutex and unlocks it when func ends
	room.mu.RLock()
	defer room.mu.RUnlock()
	//run for loop for client until clients map ends with id
	for id, client := range room.clients {
		//if client id is rqual to senderId skips him
		if id == senderId {
			continue
		}
		select {
		//sends message to client send channel
		case client.send <- message:
		default:
			log.Printf("Room broadcast: client %s buffer full", id)
		}
	}

}

// createing handlemessage func to handle message what to do when lelt, join, broadcast
func HandleMessage(client *Client, rm *RoomManager, data []byte) {
	//make empty variable of type message
	var msg Message
	//parse data which is json format data through Unmarshal and store it in msg ang if err print and return
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("Invalid message from %s: %v", client.id, err)
		return
	}
	switch msg.Type {
		//when message type is join
	case "join":
		//send room and client to JoinRoom func to join a room
		rm.JoinRoom(msg.Room, client)
		//make res variable in it write message with type joined and room this room
		res := Message{Type: "joined", Room: msg.Room}
		//make res in json format through marshal and store it in output and if there is no error send output to client send channel
		if output, err := json.Marshal(res); err == nil {
			client.send <- output
		}

		//when message type is leave
	case "leave":
		//send room and client to LeaveRoom func to leave a room
		rm.LeaveRoom(msg.Room, client)
		//make res variable in it write message with type left and room this room
		res := Message{Type: "left", Room: msg.Room}
		//make res in json format through marshal and store it in output and if there is no error send output to client send channel
		if output, err := json.Marshal(res); err == nil {
			client.send <- output
		}

		//when message type is message
	case "message":
		//send room and client id to IsMember func tocheck if client is the member of this room and it will return boolean and if not true print
		if !rm.IsMember(msg.Room, client.id.String()) {
			log.Printf("Client %s tried to send to room %s without being a member", client.id, msg.Room)
			//make res variable in it write message with type error room this room content you are not a member of this room
			res := Message{
				Type:    "error",
				Room:    msg.Room,
				Content: json.RawMessage(`"you are not a member of this room"`),
			}
			//make res in json format through marshal and store it in output and if there is no error send output to client send channel
			if output, err := json.Marshal(res); err == nil {
				client.send <- output
			}
			return
		}
		//and if is a member send room data client id to Broadcast func to broadcast it to all the others clients in the room
		rm.Broadcast(msg.Room, data, client.id.String())
	}
}

// creating readPump to read client message
func (c *Client) readPump(h *Hub, rm *RoomManager) {
	//run this func when func ends
	defer func() {
		//send client to unregister channel and clost client websocket connection
		h.unregister <- c
		c.conn.Close()
	}()

	for {
		//read cliend message and strore it in message variable and if error print Unable to read message and send client roommanager and message to HandleMessage func
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

// creating writePump to write client message
func (c *Client) writePump() {
	//run this func when func ends
	defer func() {
		//close client websocket connection
		c.conn.Close()
	}()

	//run for loop for message until all the waiting messages in client send channel
	for message := range c.send {
		//write client message with message type TextMessage and the actual message
		err := c.conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Print("Unable to write message")
			return
		}
	}
}

// creating Run func to connect and disconnect client
func (h *Hub) Run() {
	//run forever for loop
	for {
		//make select for cases
		select {
		//get client from register channel store it in client variable find client from key client in clients map make value true and print Client Connected
		case client := <-h.register:
			h.clients[client] = true
			log.Print("Client Connected")

			//get client from unregister channel store it in client delete client from client key in clients map and close client send channel and print Client disConnected
		case client := <-h.unregister:
			delete(h.clients, client)
			close(client.send)
			log.Print("Client Disconnected")
		}
	}
}

// creating serverWs for route to run
func serverWs(hub *Hub, rm *RoomManager, w http.ResponseWriter, r *http.Request) {
	//upgrade connection using upgrader variable and if err print Error Upgraging Connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("Error Upgraging Connection")
	}

	//eveytime client gets create we will add new uuid in id, websocket connection in conn, array byte channel with 256 buffersize in send
	client := &Client{
		id:   uuid.New(),
		conn: conn,
		send: make(chan []byte, 256),
	}
	//send client to register channel
	hub.register <- client

	//run readPump and writePump as gorutine means it will keep running and wait for client to write and readpump will read until server closes
	go client.readPump(hub, rm)
	go client.writePump()
}

func main() {
	//run NewHub and NewRoomManager as hub and rm
	hub := NewHub()
	rm := NewRoomManager()
	//run hub.Run as gorutine
	go hub.Run()

	//make router
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serverWs(hub, rm, w, r)
	})

	//make router for index.html
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	//run server
	log.Print("Server running on port 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Print("Error running server")
	}

}
