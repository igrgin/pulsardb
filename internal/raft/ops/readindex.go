package ops

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

type RequestIDGenerator struct {
	counter uint64
}

func NewRequestIDGenerator() *RequestIDGenerator {
	return &RequestIDGenerator{}
}

func (g *RequestIDGenerator) Next() uint64 {
	return atomic.AddUint64(&g.counter, 1)
}

func EncodeReadIndexContext(requestID uint64) []byte {
	return []byte(fmt.Sprintf("read-%d", requestID))
}

func DecodeReadIndexContext(ctx []byte) (uint64, error) {
	s := string(ctx)
	if !strings.HasPrefix(s, "read-") {
		return 0, fmt.Errorf("invalid read index context format: %s", s)
	}

	idStr := strings.TrimPrefix(s, "read-")
	return strconv.ParseUint(idStr, 10, 64)
}

func EncodeAppliedWaiterKey(index uint64, requestID uint64) string {
	return fmt.Sprintf("applied-%d-%d", index, requestID)
}
