package chunker

const DefaultChunkSize = 4 * 1024 * 1024 // pendiente de decisión final

func Split(data []byte, chunkSize int) [][]byte
func Join(chunks [][]byte) []byte
