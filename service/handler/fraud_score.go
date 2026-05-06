package handler

import (
	"service/database/ivf"
	"service/runtime"
	"sync"

	"github.com/valyala/fasthttp"
)

func clamp(x float32) float32 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func eqBytes(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}

const (
	keyUnknown = iota
	keyKnownMerchants
	keyCustomer
	keyMerchant
	keyAmount
	keyInstallments
	keyAvgAmount
	keyTxCount24h
	keyKmFromHome
	keyIsOnline
	keyCardPresent
	keyMCC
	keyID
	keyRequestedAt
	keyTimestamp
	keyKmFromCurrent
	keyLastTransaction
)

func jsonKeyID(key []byte) int {
	switch len(key) {
	case 2:
		if key[0] == 'i' && key[1] == 'd' {
			return keyID
		}
	case 3:
		if key[0] == 'm' && key[1] == 'c' && key[2] == 'c' {
			return keyMCC
		}
	case 6:
		if eqBytes(key, "amount") {
			return keyAmount
		}
	case 8:
		if key[0] == 'c' && eqBytes(key, "customer") {
			return keyCustomer
		}
		if key[0] == 'm' && eqBytes(key, "merchant") {
			return keyMerchant
		}
	case 9:
		if key[0] == 'i' && eqBytes(key, "is_online") {
			return keyIsOnline
		}
		if key[0] == 't' && eqBytes(key, "timestamp") {
			return keyTimestamp
		}
	case 10:
		if eqBytes(key, "avg_amount") {
			return keyAvgAmount
		}
	case 12:
		switch key[0] {
		case 'i':
			if eqBytes(key, "installments") {
				return keyInstallments
			}
		case 't':
			if eqBytes(key, "tx_count_24h") {
				return keyTxCount24h
			}
		case 'k':
			if eqBytes(key, "km_from_home") {
				return keyKmFromHome
			}
		case 'c':
			if eqBytes(key, "card_present") {
				return keyCardPresent
			}
		case 'r':
			if eqBytes(key, "requested_at") {
				return keyRequestedAt
			}
		}
	case 15:
		if key[0] == 'k' && eqBytes(key, "known_merchants") {
			return keyKnownMerchants
		}
		if key[0] == 'k' && eqBytes(key, "km_from_current") {
			return keyKmFromCurrent
		}
	case 16:
		if eqBytes(key, "last_transaction") {
			return keyLastTransaction
		}
	}
	return keyUnknown
}

func parseFloatFast(b []byte, i int) (float32, int) {
	if i >= len(b) {
		return 0, i
	}

	sign := float32(1)
	if b[i] == '-' {
		sign = -1
		i++
	}

	var intPart float32
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		intPart = intPart*10 + float32(b[i]-'0')
		i++
	}

	var frac float32
	base := float32(0.1)

	if i < len(b) && b[i] == '.' {
		i++
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			frac += float32(b[i]-'0') * base
			base *= 0.1
			i++
		}
	}

	return sign * (intPart + frac), i
}

func parseStringBytes(b []byte, i int) ([]byte, int) {
	// encontrar abertura "
	for i < len(b) && b[i] != '"' {
		i++
	}
	if i >= len(b) {
		return nil, i
	}
	i++ // pular "

	start := i

	for i < len(b) && b[i] != '"' {
		i++
	}
	if i >= len(b) {
		return nil, i
	}

	return b[start:i], i + 1
}

func parseHour(ts []byte) int {
	return int(ts[11]-'0')*10 + int(ts[12]-'0')
}

func parseMinuteOfDay(ts []byte) int {
	return parseHour(ts)*60 + int(ts[14]-'0')*10 + int(ts[15]-'0')
}

func parseDate(ts []byte) (y, m, d int) {
	y = int(ts[0]-'0')*1000 +
		int(ts[1]-'0')*100 +
		int(ts[2]-'0')*10 +
		int(ts[3]-'0')

	m = int(ts[5]-'0')*10 + int(ts[6]-'0')
	d = int(ts[8]-'0')*10 + int(ts[9]-'0')
	return
}

func dayOfWeek(y, m, d int) int {
	if m < 3 {
		m += 12
		y--
	}
	k := y % 100
	j := y / 100
	h := (d + (13*(m+1))/5 + k + k/4 + j/4 + 5*j) % 7
	return (h + 5) % 7
}

var march2026Weekday = [32]int{
	0,
	6, 0, 1, 2, 3, 4, 5,
	6, 0, 1, 2, 3, 4, 5,
	6, 0, 1, 2, 3, 4, 5,
	6, 0, 1, 2, 3, 4, 5,
	6, 0, 1,
}

func dayOfWeekFast(ts []byte) int {
	if len(ts) >= 10 &&
		ts[0] == '2' && ts[1] == '0' && ts[2] == '2' && ts[3] == '6' &&
		ts[5] == '0' && ts[6] == '3' {
		d := int(ts[8]-'0')*10 + int(ts[9]-'0')
		if d >= 1 && d <= 31 {
			return march2026Weekday[d]
		}
	}
	y, m, d := parseDate(ts)
	return dayOfWeek(y, m, d)
}

func parseMinutesDiff(ts1, ts2 []byte) float32 {
	return parseMinutesDiffFromCurrent(ts1, parseMinuteOfDay(ts2))
}

func parseMinutesDiffFromCurrent(ts1 []byte, currentMinute int) float32 {
	diff := currentMinute - parseMinuteOfDay(ts1)
	if diff < 0 {
		diff += 1440
	}

	return float32(diff)
}

func lookupMCC(r *runtime.RuntimeData, key []byte) float32 {
	if len(key) < 4 {
		return 0.5
	}
	if key[0] < '0' || key[0] > '9' ||
		key[1] < '0' || key[1] > '9' ||
		key[2] < '0' || key[2] > '9' ||
		key[3] < '0' || key[3] > '9' {
		return 0.5
	}

	idx := int(key[0]-'0')*1000 + int(key[1]-'0')*100 + int(key[2]-'0')*10 + int(key[3]-'0')
	return r.MCCRisk[idx]
}

func parseKnownMerchants(b []byte, i int, out [][]byte, count *int) int {
	// encontrar '['
	for i < len(b) && b[i] != '[' {
		i++
	}
	if i >= len(b) {
		*count = 0
		return i
	}
	i++ // skip [

	n := 0

	for i < len(b) {
		// pular espaços e vírgulas
		for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == ',') {
			i++
		}

		if i >= len(b) {
			break
		}

		if b[i] == ']' {
			i++
			break
		}

		if b[i] == '"' {
			start := i + 1
			i++

			for i < len(b) && b[i] != '"' {
				i++
			}
			if i >= len(b) {
				break
			}

			if n < len(out) {
				out[n] = b[start:i]
				n++
			}

			i++ // pular "
		} else {
			i++
		}
	}

	*count = n
	return i
}

func isUnknownParsed(merchantID []byte, known [][]byte, count int) float32 {
	for i := 0; i < count; i++ {
		k := known[i]

		if len(k) != len(merchantID) {
			continue
		}

		match := true
		for j := 0; j < len(k); j++ {
			if k[j] != merchantID[j] {
				match = false
				break
			}
		}

		if match {
			return 0
		}
	}
	return 1
}

func buildVectorUltra(body []byte, r *runtime.RuntimeData, out *ivf.Vector) {
	*out = ivf.Vector{}
	n := &r.Norm

	var amount, installments float32
	var avgAmount, merchantAvg float32
	var txCount float32
	var kmHome float32

	var isOnline, cardPresent bool

	var merchantID []byte
	var mcc []byte

	var hasLastTx bool
	var lastTS []byte
	var currTS []byte
	var kmLast float32

	var section int

	var knownCount int
	var known [32][]byte // buffer fixo (ajuste se quiser)
	currMinute := -1

	const (
		secNone = iota
		secCustomer
		secMerchant
	)

	i := 0

	for i < len(body) {
		if body[i] != '"' {
			i++
			continue
		}

		i++
		start := i

		for i < len(body) && body[i] != '"' {
			i++
		}
		if i >= len(body) {
			break
		}

		key := body[start:i]
		i++

		for i < len(body) && body[i] != ':' {
			i++
		}
		if i >= len(body) {
			break
		}
		i++

		for i < len(body) && body[i] == ' ' {
			i++
		}

		switch jsonKeyID(key) {
		case keyKnownMerchants:
			i = parseKnownMerchants(body, i, known[:], &knownCount)
		case keyCustomer:
			section = secCustomer

		case keyMerchant:
			section = secMerchant

		case keyAmount:
			amount, i = parseFloatFast(body, i)

		case keyInstallments:
			installments, i = parseFloatFast(body, i)

		case keyAvgAmount:
			val, ni := parseFloatFast(body, i)
			i = ni

			if section == secCustomer {
				avgAmount = val
			} else if section == secMerchant {
				merchantAvg = val
			}

		case keyTxCount24h:
			txCount, i = parseFloatFast(body, i)

		case keyKmFromHome:
			kmHome, i = parseFloatFast(body, i)

		case keyIsOnline:
			isOnline = body[i] == 't'
			i += 4

		case keyCardPresent:
			cardPresent = body[i] == 't'
			i += 4

		case keyMCC:
			mcc, i = parseStringBytes(body, i)

		case keyID:
			if section == secMerchant {
				merchantID, i = parseStringBytes(body, i)
			}

		case keyRequestedAt:
			currTS, i = parseStringBytes(body, i)

		case keyTimestamp:
			lastTS, i = parseStringBytes(body, i)

		case keyKmFromCurrent:
			kmLast, i = parseFloatFast(body, i)

		case keyLastTransaction:
			if body[i] == 'n' {
				hasLastTx = false
				i += 4
			} else {
				hasLastTx = true
			}
		}

		i++
	}

	// --- VECTOR ---

	out[0] = clamp(amount / n.MaxAmount)
	out[1] = clamp(installments / n.MaxInstallments)
	if avgAmount > 0 {
		out[2] = clamp((amount / avgAmount) / n.AmountVsAvgRatio)
	} else {
		out[2] = 0
	}

	// 🔥 timestamp otimizado
	if len(currTS) >= 16 {
		currMinute = parseMinuteOfDay(currTS)
		h := currMinute / 60
		out[3] = float32(h) / 23.0

		wd := dayOfWeekFast(currTS)
		out[4] = float32(wd) / 6.0
	}

	if !hasLastTx || len(lastTS) == 0 || currMinute < 0 {
		out[5] = -1
		out[6] = -1
	} else {
		mins := parseMinutesDiffFromCurrent(lastTS, currMinute)
		out[5] = clamp(mins / n.MaxMinutes)
		out[6] = clamp(kmLast / n.MaxKm)
	}

	out[7] = clamp(kmHome / n.MaxKm)
	out[8] = clamp(txCount / n.MaxTxCount24h)

	if isOnline {
		out[9] = 1
	}
	if cardPresent {
		out[10] = 1
	}

	if len(merchantID) == 0 {
		out[11] = 1
	} else {
		out[11] = isUnknownParsed(merchantID, known[:], knownCount)
	}
	out[12] = lookupMCC(r, mcc)
	out[13] = clamp(merchantAvg / n.MaxMerchantAvgAmount)
}

var fraudResponses = [6][]byte{
	[]byte(`{"approved":true,"fraud_score":0.0}`),
	[]byte(`{"approved":true,"fraud_score":0.2}`),
	[]byte(`{"approved":true,"fraud_score":0.4}`),
	[]byte(`{"approved":false,"fraud_score":0.6}`),
	[]byte(`{"approved":false,"fraud_score":0.8}`),
	[]byte(`{"approved":false,"fraud_score":1.0}`),
}

type requestState struct {
	q  ivf.Vector
	ws ivf.SearchWorkspace
}

func writeFraudCountResponse(ctx *fasthttp.RequestCtx, fraudCount int) {
	if fraudCount < 0 {
		fraudCount = 0
	} else if fraudCount > 5 {
		fraudCount = 5
	}
	ctx.Response.SetBodyRaw(fraudResponses[fraudCount])
}

func Handler(rt *runtime.RuntimeData) fasthttp.RequestHandler {
	pool := sync.Pool{
		New: func() any {
			return new(requestState)
		},
	}

	return func(ctx *fasthttp.RequestCtx) {
		state := pool.Get().(*requestState)
		buildVectorUltra(ctx.Request.Body(), rt, &state.q)
		fraudCount := rt.DB.FraudCount5WithWorkspace(&state.q, &state.ws)
		pool.Put(state)
		writeFraudCountResponse(ctx, fraudCount)
	}
}
