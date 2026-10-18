package chunker

import (
	"bytes"
	"testing"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		chunkSize int
		want      [][]byte
	}{
		{
			name:      "archivo dividido exactamente",
			data:      []byte("abcdef"),
			chunkSize: 2,
			want: [][]byte{
				[]byte("ab"),
				[]byte("cd"),
				[]byte("ef"),
			},
		},
		{
			name:      "ultimo chunk mas pequeno",
			data:      []byte("abcdefgh"),
			chunkSize: 3,
			want: [][]byte{
				[]byte("abc"),
				[]byte("def"),
				[]byte("gh"),
			},
		},
		{
			name:      "archivo mas pequeno que chunkSize",
			data:      []byte("abc"),
			chunkSize: 10,
			want: [][]byte{
				[]byte("abc")},
		},
		{
			name:      "archivo vacio",
			data:      []byte{},
			chunkSize: 3,
			want:      nil,
		},
		{
			name:      "chunkSize invalido",
			data:      []byte("abc"),
			chunkSize: 0,
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Split(tt.data, tt.chunkSize)

			if len(got) != len(tt.want) {
				t.Fatalf("Split() produjo %d chunks; se esperaban %d",
					len(got), len(tt.want))
			}

			for i := range tt.want {
				if !bytes.Equal(got[i], tt.want[i]) { // como es de tipo [][]byte, entonces got[i] devuelve el slice completo que está en la posición i.
					t.Errorf("chunk %d = %q; se esperaba %q",
						i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestJoin(t *testing.T) {
	chunks := [][]byte{
		[]byte("abc"),
		[]byte("def"),
		[]byte("gh"),
	}

	got := Join(chunks)
	want := []byte("abcdefgh")

	if !bytes.Equal(got, want) {
		t.Errorf("Join() = %q; se esperaba %q", got, want)
	}
}

func TestSplitJoinRoundTrip(t *testing.T) {
	original := []byte("contenido de prueba para DFSha")

	chunks := Split(original, 5)
	reconstructed := Join(chunks)

	if !bytes.Equal(reconstructed, original) {
		t.Fatalf("el archivo reconstruido no coincide con el original")
	}
}
