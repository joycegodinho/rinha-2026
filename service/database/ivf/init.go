package ivf

import (
	"log"
	"os"
)

func InitDB(indexPath, binPath, jsonPath string) *IVF {
	log.Print("Init IVF DB")

	if _, err := os.Stat(indexPath); err == nil {
		db, err := LoadKMeansIndex(indexPath)
		if err == nil {
			log.Print("Loaded kmeans IVF index")
			return db
		}
		log.Printf("Could not load kmeans IVF index, falling back: %v", err)
	}

	if _, err := os.Stat(binPath); err == nil {
		db, err := LoadBinary(binPath)
		if err == nil {
			return db
		}
		log.Printf("Could not load IVF binary, rebuilding: %v", err)
	}

	log.Print("Building IVF from JSON")

	db := BuildFromJSON(jsonPath)

	if err := db.SaveBinary(binPath); err != nil {
		panic(err)
	}

	log.Print("IVF saved")

	return db
}
