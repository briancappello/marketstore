package functions

import (
	"math"
	"math/bits"
)

func SumFloat32(values []float32) float32 {
	sum := float32(0)
	for _, val := range values {
		if sum > math.MaxFloat32-val {
			panic("float overflow")
		}
		sum += val
	}
	return sum
}

func SumFloat32toFloat64(values []float32) float64 {
	sum := float64(0)
	for _, val := range values {
		sum += float64(val)
	}
	return sum
}

func SumFloat64(values []float64) float64 {
	sum := float64(0)
	for _, val := range values {
		if sum > math.MaxFloat64-val {
			panic("float overflow")
		}
		sum += val
	}
	return sum
}

func SumInt8(values []int8) int8 {
	sum := int8(0)
	for _, val := range values {
		if sum > math.MaxInt8-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt8toInt16(values []int8) int16 {
	sum := int16(0)
	for _, val := range values {
		sum += int16(val)
	}
	return sum
}

func SumInt16(values []int16) int16 {
	sum := int16(0)
	for _, val := range values {
		if sum > math.MaxInt16-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt16toInt32(values []int16) int32 {
	sum := int32(0)
	for _, val := range values {
		sum += int32(val)
	}
	return sum
}

func SumInt(values []int) int {
	sum := int(0)
	max := math.MaxInt32
	if bits.UintSize == 64 {
		max = math.MaxInt64
	}
	for _, val := range values {
		if sum > max-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt32(values []int32) int32 {
	sum := int32(0)
	for _, val := range values {
		if sum > math.MaxInt32-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt32toInt64(values []int32) int64 {
	sum := int64(0)
	for _, val := range values {
		sum += int64(val)
	}
	return sum
}

func SumInt64(values []int64) int64 {
	sum := int64(0)
	for _, val := range values {
		if sum > math.MaxInt64-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint(values []uint) uint {
	sum := uint(0)
	max := uint(math.MaxUint32)
	if bits.UintSize == 64 {
		max = math.MaxUint64
	}
	for _, val := range values {
		if sum > max-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint8(values []uint8) uint8 {
	sum := uint8(0)
	for _, val := range values {
		if sum > math.MaxUint8-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint16(values []uint16) uint16 {
	sum := uint16(0)
	for _, val := range values {
		if sum > math.MaxUint16-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint32(values []uint32) uint32 {
	sum := uint32(0)
	for _, val := range values {
		if sum > math.MaxUint32-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint8toUint16(values []uint8) uint16 {
	sum := uint16(0)
	for _, val := range values {
		sum += uint16(val)
	}
	return sum
}

func SumUint16toUint32(values []uint16) uint32 {
	sum := uint32(0)
	for _, val := range values {
		sum += uint32(val)
	}
	return sum
}

func SumUint32toUint64(values []uint32) uint64 {
	sum := uint64(0)
	for _, val := range values {
		sum += uint64(val)
	}
	return sum
}

func SumUint64(values []uint64) uint64 {
	sum := uint64(0)
	for _, val := range values {
		if sum > math.MaxUint64-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}
