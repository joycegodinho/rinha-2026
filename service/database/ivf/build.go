package ivf

import (
	"encoding/json"
	"log"
	"os"
)

func toVector16(v [14]float32) Vector {
	var out Vector
	for i := 0; i < 14; i++ {
		out[i] = v[i]
	}
	return out
}

func loadJSON(path string) (*IVF, error) {
	log.Print("Start loading IVF from json")

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	if _, err := dec.Token(); err != nil {
		return nil, err
	}

	db := &IVF{
		Vectors: make([]int16, 0, 3_000_000*Dim),
		Labels:  make([]uint8, 0, 3_000_000),
		Offsets: make([]uint32, BucketCount+1),
	}

	buckets := make([][]uint32, BucketCount)

	for dec.More() {
		var rec struct {
			Vector [14]float32 `json:"vector"`
			Label  string      `json:"label"`
		}

		if err := dec.Decode(&rec); err != nil {
			return nil, err
		}

		vec := toVector16(rec.Vector)
		q := Quantize(&vec)
		label := Legit
		if rec.Label == "fraud" {
			label = Fraud
		}

		id := uint32(len(db.Labels))
		for i := 0; i < Dim; i++ {
			db.Vectors = append(db.Vectors, q[i])
		}
		db.Labels = append(db.Labels, label)

		key := coarseKey(&q)
		buckets[key] = append(buckets[key], id)

	}

	var total uint32
	for i := 0; i < BucketCount; i++ {
		db.Offsets[i] = total
		total += uint32(len(buckets[i]))
	}
	db.Offsets[BucketCount] = total

	db.IDs = make([]uint32, 0, total)
	for i := 0; i < BucketCount; i++ {
		db.IDs = append(db.IDs, buckets[i]...)
	}

	log.Printf("IVF loaded: vectors=%d buckets=%d ids=%d", len(db.Labels), BucketCount, len(db.IDs))

	return db, nil
}

func BuildFromJSON(path string) *IVF {
	db, err := loadJSON(path)
	if err != nil {
		panic(err)
	}
	return db
}
