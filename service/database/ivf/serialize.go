package ivf

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"syscall"
	"unsafe"
)

const magic uint32 = 0x32465649 // IVF2

type header struct {
	Magic       uint32
	N           uint32
	Dim         uint32
	BucketCount uint32
	IDCount     uint32
	Reserved    uint32
}

func (db *IVF) SaveBinary(path string) error {
	log.Printf("Saving IVF to binary file %s", path)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)

	h := header{
		Magic:       magic,
		N:           uint32(len(db.Labels)),
		Dim:         Dim,
		BucketCount: BucketCount,
		IDCount:     uint32(len(db.IDs)),
	}

	if err := binary.Write(w, binary.LittleEndian, h); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, db.Vectors); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, db.Labels); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, db.Offsets); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, db.IDs); err != nil {
		return err
	}

	return w.Flush()
}

func LoadBinary(path string) (*IVF, error) {
	log.Printf("Loading IVF from binary file %s", path)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(info.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	if len(data) < int(unsafe.Sizeof(header{})) {
		_ = syscall.Munmap(data)
		return nil, fmt.Errorf("invalid IVF binary size")
	}

	h := *(*header)(unsafe.Pointer(&data[0]))
	if h.Magic != magic || h.Dim != Dim || h.BucketCount != BucketCount {
		_ = syscall.Munmap(data)
		return nil, fmt.Errorf("invalid IVF binary header")
	}

	pos := int(unsafe.Sizeof(header{}))
	vectorCount := int(h.N) * Dim
	vectorBytes := vectorCount * 2
	labelBytes := int(h.N)
	offsetCount := BucketCount + 1
	offsetBytes := offsetCount * 4
	idBytes := int(h.IDCount) * 4

	need := pos + vectorBytes + labelBytes + offsetBytes + idBytes
	if len(data) < need {
		_ = syscall.Munmap(data)
		return nil, fmt.Errorf("invalid IVF binary payload")
	}

	vectors := unsafe.Slice((*int16)(unsafe.Pointer(&data[pos])), vectorCount)
	pos += vectorBytes

	labels := data[pos : pos+labelBytes]
	pos += labelBytes

	offsets := unsafe.Slice((*uint32)(unsafe.Pointer(&data[pos])), offsetCount)
	pos += offsetBytes

	ids := unsafe.Slice((*uint32)(unsafe.Pointer(&data[pos])), int(h.IDCount))

	db := &IVF{
		Vectors: vectors,
		Labels:  labels,
		Offsets: offsets,
		IDs:     ids,
		mmap:    data,
	}

	return db, nil
}
