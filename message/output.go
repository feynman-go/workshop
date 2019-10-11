package message

type OutputMessage struct {
	OffsetToken string
	Topic       string
	Message
}

type Message struct {
	UID       string
	Partition int64
	PayLoad   []byte
	Head      map[string]string
}

