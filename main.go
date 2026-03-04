package main

import (
	"database/sql"
	//"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"strings"
	"strconv"
	"sync"
	"time"
	"encoding/json"
	"encoding/hex"
	"math/rand"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"os"
)






var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	onlineUsers   = make(map[int]int)
	onlineUsersMu sync.Mutex
	db            *sql.DB
	
)


var reverseHtmlReplacer = strings.NewReplacer(
	"&amp;",  "&",
	"&lt;",  "<",
	"&gt;",  ">",
	"&quot;",  `"`,
	"&#39;",  "'",
)


func main() {
	var err error
	
  db, err = sql.Open(
    "mysql", 
    "master:master@unix(/run/mysqld/mysqld.sock)/db?parseTime=true",
  )

  db.SetMaxOpenConns(50)
  db.SetMaxIdleConns(50)
  db.SetConnMaxLifetime(5 * time.Minute)

  

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
  
  http.HandleFunc("/!message", func (w http.ResponseWriter, r *http.Request){
    ip := strings.Split(r.RemoteAddr,":")[0]
    if(ip!="127.0.0.1"){
      return;
    }

    query      := r.URL.Query()
    action     := query.Get("action")
    message    := query.Get("message")
		group_id   := ToInt(query.Get("group_id"))
		channel_id := ToInt(query.Get("channel_id"))
		post_id    := ToInt(query.Get("post_id"))
		user_id    := ToInt(query.Get("user_id"))



		var sb strings.Builder
		var json_message string
		if(action=="post/add" || action=="post/mention"){
			sb.WriteString(`{"action":"`)
			sb.WriteString(action)
			sb.WriteString(`","message":"`)
			sb.WriteString(message)
			sb.WriteString(`","group_id":`)
			sb.WriteString(strconv.Itoa(group_id))
			sb.WriteString(`,"channel_id":`)
			sb.WriteString(strconv.Itoa(channel_id))
			sb.WriteString(`,"post_id":`)
			sb.WriteString(strconv.Itoa(post_id))
			sb.WriteString(`,"user_id":`)
			sb.WriteString(strconv.Itoa(user_id))
			sb.WriteString(`}`)
			json_message = sb.String()

			bot_ids := BotInGroup(group_id)
			for _,bot_id := range bot_ids {
				Send(bot_id,json_message)
			}
		}

		if(action=="message/send"){
			sb.WriteString(`{"action":"`)
			sb.WriteString(action)
			sb.WriteString(`","message":"`)
			sb.WriteString(message)
			sb.WriteString(`","user_id":`)
			sb.WriteString(strconv.Itoa(user_id))
			sb.WriteString(`}`)
			json_message = sb.String()

			Send(channel_id,json_message)
		}



		if(action=="post/bumote"){
			sb.WriteString(`{"action":"`)
			sb.WriteString(action)
			sb.WriteString(`","message":`)
			message = reverseHtmlReplacer.Replace(message)
			sb.WriteString(message)
			sb.WriteString(`,"post_id":`)
			sb.WriteString(strconv.Itoa(post_id))
			sb.WriteString(`,"user_id":`)
			sb.WriteString(strconv.Itoa(user_id))
			sb.WriteString(`}`)
			json_message = sb.String()

			Send(group_id,json_message)
		}



		
		
		if(action=="group/join" || action=="group/leave" || action=="group/kick" ){
			sb.WriteString(`{"action":"`)
			sb.WriteString(action)
			sb.WriteString(`","group_id":`)
			sb.WriteString(strconv.Itoa(group_id))
			sb.WriteString(`,"user_id":`)
			sb.WriteString(strconv.Itoa(user_id))
			sb.WriteString(`}`)
			json_message = sb.String()

			bot_ids := BotInGroup(group_id)
			for _,bot_id := range bot_ids {
				Send(bot_id,json_message)
			}
		}

		

    //!HERE CONTINUE


    


  })

	http.HandleFunc("/!bot", SocketHandler)
	port := argument("port")
	table("Server started at :"+port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

var bots sync.Map

type Client struct {
	conn *websocket.Conn
	send chan []byte
}

func Send(userID int, message string) bool {
	val, ok := bots.Load(userID)
	if !ok {
		return false
	}

	client := val.(*Client)

	select {
	case client.send <- []byte(message):
		return true
	default:
		// Slow consumer -> bağlantıyı düşür
		bots.Delete(userID)
		client.conn.Close()
		return false
	}
}

func SocketHandler(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	ws.SetReadLimit(1024)
	ws.SetReadDeadline(time.Now().Add(60 * time.Second))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// İlk mesajdan userID al
	_, message, err := ws.ReadMessage()
	if err != nil {
		write("ws.Close()")
		ws.Close()
		return
	}

	userID := Func_User_ID(r, string(message))
	if ( userID < 1 ) {
    ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
    err := ws.WriteMessage(websocket.TextMessage, []byte("\"AUTH_PROBLEM\""))
    if err != nil {
        ws.Close()
        return
    }
    ws.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "\"AUTH_PROBLEM\""),
			time.Now().Add(time.Second),
    )
    ws.Close()
    return
	}

	
	write(userID ," connected")

	client := &Client{
		conn: ws,
		send: make(chan []byte, 256),
	}

	// Eğer aynı userID varsa eski bağlantıyı kapat
	if old, ok := bots.Load(userID); ok {
		oldClient := old.(*Client)
		oldClient.conn.Close()
		bots.Delete(userID)
	}

	bots.Store(userID, client)


	// ----- PING -----
	ticker := time.NewTicker(30 * time.Second)
	go func(c *Client) {
		for range ticker.C {
			// Bağlantı kapandıysa ticker durdur
			if err := c.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second)); err != nil {
				ticker.Stop()
				return
			}
		}
	}(client)

	// WRITE GOROUTINE (tek writer, channel asla kapanmaz)
	go func(c *Client) {
		for {
			msg, ok := <-c.send
			if !ok {
				// Bu durumda aslında hiç olmayacak (biz kapatmıyoruz)
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}(client)


	Send(userID,"\"CONNECTED\"")

	// READ LOOP (tek reader)
	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			break
		}
	}

	// Temizlik
	bots.Delete(userID)
	ws.Close()
}





func Func_User_IP_Address(r *http.Request) string {
	headers := []string{"CF-Connecting-IP", "X-Forwarded-For", "X-Real-IP"}
	for _, h := range headers {
		ips := r.Header.Get(h)
		if ips != "" {
			return strings.TrimSpace(strings.Split(ips, ",")[0])
		}
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

func Func_User_New_Negative_Hash_Number(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return -int(h.Sum32() & 0x7FFFFFFF)
}

func Func_User_ID(r *http.Request, token string) int {
	var user_id int
	if token != "" {
		db.QueryRow("SELECT `user_id` FROM `session` WHERE `token` = ? AND `expire` > UNIX_TIMESTAMP()", token).Scan(&user_id)
	}
	if user_id == 0 {
		user_id = Func_User_New_Negative_Hash_Number(Func_User_IP_Address(r))
	}
	return user_id
}



type Struct_User struct {
  ID                        int         `db:"id" json:"id"`   
  Name                      string      `db:"name" json:"name"` 
  Nick                      string      `db:"nick" json:"nick"`
  Image                     string      `db:"image" json:"image"`
	Hash                      string      `db:"hash" json:"hash"`
}
var user_query string = "SELECT id,name,nick,image FROM `user` WHERE id=? AND blocked=0"
func Func_User_Info(id int) *Struct_User  {
	var s Struct_User
	db.QueryRow(user_query, id).Scan(
    &s.ID,
    &s.Name,
    &s.Nick,
    &s.Image,
  )
	s.ID = id
	return &s
}







func contains(arr []int, val int) bool {
  for _, v := range arr {
    if v == val {
      return true
    }
  }
  return false
}

func intersect(a, b []int) []int {
  m := make(map[int]bool)
  var result []int
  for _, v := range a {
    m[v] = true
  }
  for _, v := range b {
    if m[v] {
      result = append(result, v)
    }
  }
  return result
}


func SplitInt(s string) []int {
  if s == "" {
    return []int{}
  }

  parts := strings.Split(s, ",")
  var result []int
  for _, part := range parts {
    trimmed := strings.TrimSpace(part)
    if trimmed == "" {
      continue
    }
    if num, err := strconv.Atoi(trimmed); err == nil {
      result = append(result, num)
    }
  }
  return result
}



func Func_Permission_Channel(channel_id int, user_id int) int {
  var channel_GroupID int
  var channel_ReadRole, channel_WriteRole, channel_ControlRole, channel_PlusUser, channel_MinusUser string
  var channel_Type int


  //@ Main Blocked
  var blocked int
  db.QueryRow("SELECT `blocked` FROM `user` WHERE `id` = ? LIMIT 1", user_id).Scan(&blocked)
  if(blocked!=0){
    return 0
  }

  //@ MemberBlocked
  

  err := db.QueryRow("SELECT `group_id`,`read_role_ids`,`write_role_ids`,`control_role_ids`,`plus_user_ids`,`minus_user_ids`,`type` FROM `channel` WHERE `id` = ? LIMIT 1", channel_id).Scan(
    &channel_GroupID,
    &channel_ReadRole,
    &channel_WriteRole,
    &channel_ControlRole,
    &channel_PlusUser,
    &channel_MinusUser,
    &channel_Type,
  )
  if err != nil {
    return 0
  }


  group_id := channel_GroupID
  var group_owner_number int;
  db.QueryRow("SELECT `owner_id` FROM `group` WHERE `id` = ?", group_id).Scan(&group_owner_number)


  userIsMember := false
  var rolesRow string
  var member_block int
  err = db.QueryRow("SELECT `member`.`role_ids`,`member`.block FROM `member` WHERE `member`.`group_id` = ? AND `member`.`user_id` = ? LIMIT 1", group_id, user_id).Scan(&rolesRow,&member_block)

  if(member_block!=0){
    return 0
  }

  userRoles := ""
  if err==nil {
      userRoles = rolesRow
      userIsMember = true
  }

  userRolesArr := SplitInt(userRoles)
  readRoles := SplitInt(channel_ReadRole)
  writeRoles := SplitInt(channel_WriteRole)
  controlRoles := SplitInt(channel_ControlRole)
  plusUsers := SplitInt(channel_PlusUser)
  minusUsers := SplitInt(channel_MinusUser)


  // Group owner
  if group_owner_number == user_id {
    return 7
  }

  // Explicitly authorized
  for _, uid := range plusUsers {
      if uid == user_id {
          return 1
      }
  }

  // Explicitly banned
  for _, uid := range minusUsers {
      if uid == user_id {
          return 0
      }
  }

  // Permission calculation
  power := 0

  if contains(readRoles, -1) {
      power += 1
  } else if contains(readRoles, 0) && userIsMember {
      power += 1
  } else if len(intersect(userRolesArr, readRoles)) > 0 {
      power += 1
  }

  if user_id > 0 && !contains([]int{2, 10, 20, -20}, channel_Type) {
      if contains(writeRoles, -1) {
          power += 2
      } else if contains(writeRoles, 0) && userIsMember {
          power += 2
      } else if len(intersect(userRolesArr, writeRoles)) > 0 {
          power += 2
      }
  }

  if user_id > 0 && !contains([]int{10, 20}, channel_Type) {
      if len(intersect(userRolesArr, controlRoles)) > 0 {
          power += 4
      }
  }

  return power
}





func BotInGroup(group_id int) []int{
	/*
  list_1 := SelectInts("SELECT member.user_id FROM member  INNER JOIN app ON member.user_id=app.user_id WHERE app.app_type_id=11 AND member.group_id=?",group_id)
	list_2 := SelectInts("SELECT member.user_id FROM member LEFT JOIN user ON user.id=member.user_id WHERE (user.nick LIKE 'bot%' OR user.nick LIKE '%bot') AND member.group_id=?",group_id)
	list   := append(list_1,list_2...)
	return list
	*/
	return SelectInts(`
		SELECT DISTINCT m.user_id
		FROM member m
		LEFT JOIN app a 
			ON m.user_id = a.user_id AND a.app_type_id = 11
		LEFT JOIN user u 
			ON u.id = m.user_id
		WHERE m.group_id = ?
		  AND (
				a.user_id IS NOT NULL
				OR u.nick LIKE 'bot%'
				OR u.nick LIKE '%bot'
		  )
	`, group_id)
}





func SelectInts(query string, args ...interface{}) []int {
  rows, err := db.Query(query, args...)
  if err != nil {
    log.Println("SelectInts error:", err, "Query:", query, "Args:", args)
    return []int{}
  }
  defer rows.Close()

  var results []int
  for rows.Next() {
    var value int
    if err := rows.Scan(&value); err != nil {
      log.Println("SelectInts scan error:", err)
      continue
    }
    results = append(results, value)
  }

  if err := rows.Err(); err != nil {
    log.Println("SelectInts rows error:", err)
  }

  return results
}



func argument(a string) string{
	response := ""
	for _,arg := range os.Args{
		if(strings.HasPrefix(arg, a+"=") ){
			response=strings.Split(arg, "=")[1]
			response=strings.Trim(response, "\"")
		}
	}
	return response
}


func write(values ...interface{}) {
	originalFlags := log.Flags()
	log.SetFlags(0)
	log.Println(values...)
	log.SetFlags(originalFlags)
}
func center(text string) string {
	const width = 48
	if len(text) >= width {
		return text // return as-is if longer than width
	}
	padding := (width - len(text)) / 2
	return strings.Repeat(" ", padding) + text + strings.Repeat(" ", width-len(text)-padding)
}
func table(name string){
	// █
	write("╔════════════════════════════════════════════════╗") // 50 Char
	write("║"+center(name)+"║")
	write("╚════════════════════════════════════════════════╝") // 50 Char
}
func row(name string){
	// █
	write("┌────────────────────────────────────────────────┐") // 50 Char
	write("│"+center(name)+"│")
	write("└────────────────────────────────────────────────┘") // 50 Char
}



func random_hex(digit int) string {
	if digit%2 != 0 {
		digit++ // hex string needs even number of digits (2 hex chars per byte)
	}
	
	bytes := make([]byte, digit/2)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)[:digit]
}



func ToInt(number string) int {
	i, err := strconv.Atoi(number)
	if err != nil {
		return 0
	}
	return i
}
func ToString(number int) string {
	return strconv.Itoa(number)
}
func ToJson(data interface{}) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return "{}" 
	}
	return string(bytes)
}
