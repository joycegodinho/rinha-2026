package handler

import (
	"service/database/ivf"
	"service/runtime"
	"sync"
)

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

type Classifier struct {
	rt   *runtime.RuntimeData
	pool sync.Pool
}

func NewClassifier(rt *runtime.RuntimeData) *Classifier {
	c := &Classifier{rt: rt}
	c.pool.New = func() any {
		return new(requestState)
	}
	return c
}

func clampFraudCount(fraudCount int) int {
	if fraudCount < 0 {
		return 0
	}
	if fraudCount > 5 {
		return 5
	}
	return fraudCount
}

func FraudResponse(fraudCount int) []byte {
	return fraudResponses[clampFraudCount(fraudCount)]
}

func (c *Classifier) FraudCount(body []byte) int {
	state := c.pool.Get().(*requestState)
	buildVectorUltra(body, c.rt, &state.q)
	fraudCount := c.rt.DB.FraudCount5WithWorkspace(&state.q, &state.ws)
	c.pool.Put(state)
	return fraudCount
}

