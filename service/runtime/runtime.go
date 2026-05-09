package runtime

import (
	"encoding/json"
	"log"
	"os"
	"service/database/ivf"
	"time"
)

type RuntimeData struct {
	DB *ivf.IVF

	MCCRisk [10000]float32

	Norm NormConfig
}

type MCCEntry struct {
	Key   [4]byte // ex: "5411"
	Value float32
}

type NormConfig struct {
	MaxAmount            float32
	MaxInstallments      float32
	AmountVsAvgRatio     float32
	MaxMinutes           float32
	MaxKm                float32
	MaxTxCount24h        float32
	MaxMerchantAvgAmount float32
}

type NormJSON struct {
	MaxAmount            float32 `json:"max_amount"`
	MaxInstallments      float32 `json:"max_installments"`
	AmountVsAvgRatio     float32 `json:"amount_vs_avg_ratio"`
	MaxMinutes           float32 `json:"max_minutes"`
	MaxKm                float32 `json:"max_km"`
	MaxTxCount24h        float32 `json:"max_tx_count_24h"`
	MaxMerchantAvgAmount float32 `json:"max_merchant_avg_amount"`
}

func loadNorm(path string, rt *RuntimeData) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var n NormJSON
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}

	rt.Norm = NormConfig{
		MaxAmount:            n.MaxAmount,
		MaxInstallments:      n.MaxInstallments,
		AmountVsAvgRatio:     n.AmountVsAvgRatio,
		MaxMinutes:           n.MaxMinutes,
		MaxKm:                n.MaxKm,
		MaxTxCount24h:        n.MaxTxCount24h,
		MaxMerchantAvgAmount: n.MaxMerchantAvgAmount,
	}

	return nil
}

func loadMCC(path string, rt *RuntimeData) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var tmp map[string]float32
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	for i := range rt.MCCRisk {
		rt.MCCRisk[i] = 0.5
	}

	for k, v := range tmp {
		if len(k) != 4 {
			continue
		}
		idx := int(k[0]-'0')*1000 + int(k[1]-'0')*100 + int(k[2]-'0')*10 + int(k[3]-'0')
		if idx >= 0 && idx < len(rt.MCCRisk) {
			rt.MCCRisk[idx] = v
		}
	}

	return nil
}

func Init() *RuntimeData {
	rt := &RuntimeData{}

	indexPath := os.Getenv("INDEX_PATH")
	if indexPath == "" {
		indexPath = "service/index.bin.gz"
	}

	db := ivf.InitDB(
		indexPath,
		"service/ivf.bin",
		"service/references.json",
	)

	rt.DB = db
	if os.Getenv("DB_WARMUP") != "off" {
		start := time.Now()
		rt.DB.Warmup()
		log.Printf("IVF warmup finished in %s", time.Since(start))
	}

	if err := loadNorm("service/normalization.json", rt); err != nil {
		panic(err)
	}

	if err := loadMCC("service/mcc_risk.json", rt); err != nil {
		panic(err)
	}

	return rt
}
