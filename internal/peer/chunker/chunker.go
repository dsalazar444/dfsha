package chunker

func Split(data []byte, chunkSize int) [][]byte {
	if chunkSize <= 0 {
		return nil
	}

	var chunks [][]byte

	for start := 0; start < len(data); start += chunkSize {
		end := min(start+chunkSize, len(data))

		chunk := data[start:end]
		chunks = append(chunks, chunk)
	}

	return chunks
}

func Join(chunks [][]byte) []byte {
	var data []byte

	for _, chunk := range chunks {
		// ... Es para añadir el chunk como un elemento, y quede en 1d []. Sin el
		// se añadiria en diferentes niveles, quedando una matriz. Es como append vs extend en py
		data = append(data, chunk...)
	}

	return data
}
