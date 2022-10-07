package replication

type MessageType string

type Chunk struct {
	Owner    string
	Category string
	FileName string
}

type Message struct {
	Type  MessageType
	Chunk Chunk
}
