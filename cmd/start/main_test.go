package start

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReplacePort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hostPort string
		newPort  string
		expected string
	}{
		{
			name:     "empty host with port",
			hostPort: ":5993",
			newPort:  "6000",
			expected: ":6000",
		},
		{
			name:     "localhost with port",
			hostPort: "localhost:5993",
			newPort:  "7000",
			expected: "localhost:7000",
		},
		{
			name:     "ip address with port",
			hostPort: "0.0.0.0:5993",
			newPort:  "8080",
			expected: "0.0.0.0:8080",
		},
		{
			name:     "replace grpc port",
			hostPort: ":5995",
			newPort:  "9000",
			expected: ":9000",
		},
		{
			name:     "host without port falls back gracefully",
			hostPort: "localhost",
			newPort:  "5993",
			expected: "localhost:5993",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := replacePort(tt.hostPort, tt.newPort)
			assert.Equal(t, tt.expected, result)
		})
	}
}
