package message

type OutputMessage struct {
	OffsetToken string
	Topic       string
	Message
}

type Message struct {
	Key 	  string
	PayLoad   []byte
	Head      map[string]string
}

