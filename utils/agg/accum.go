package agg

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type accumParam struct {
	inputName, funcName, outputName string
}

type accumGroup struct {
	accumulators []*accumulator
	params       []accumParam
}

type accumulator struct {
	ivalues interface{} // input column(s)
	iout    interface{} // output slice
	ifunc   interface{} // function
}

func newAccumGroup(cs *io.ColumnSeries, params []accumParam) *accumGroup {
	var accumulators []*accumulator
	for _, param := range params {
		accumulator := newAccumulator(cs, param)
		accumulators = append(accumulators, accumulator)
	}
	return &accumGroup{
		accumulators: accumulators,
		params:       params,
	}
}

func (ag *accumGroup) apply(start, end int) error {
	for _, accumulator := range ag.accumulators {
		err := accumulator.apply(start, end)
		if err != nil {
			return fmt.Errorf("apply to accumulator. start=%d, end=%d: %w", start, end, err)
		}
	}
	return nil
}

func (ag *accumGroup) addColumns(cs *io.ColumnSeries) {
	for i, param := range ag.params {
		cs.AddColumn(param.outputName, ag.accumulators[i].iout)
	}
}

// Accumulator functions for different types

func firstFloat32(values []float32) float32 { return values[0] }
func lastFloat32(values []float32) float32  { return values[len(values)-1] }
func maxFloat32(values []float32) float32 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
func minFloat32(values []float32) float32 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
func sumFloat32(values []float32) float32 {
	var sum float32
	for _, v := range values {
		sum += v
	}
	return sum
}

func firstFloat64(values []float64) float64 { return values[0] }
func lastFloat64(values []float64) float64  { return values[len(values)-1] }
func maxFloat64(values []float64) float64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
func minFloat64(values []float64) float64 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
func sumFloat64(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum
}

func firstInt32(values []int32) int32 { return values[0] }
func lastInt32(values []int32) int32  { return values[len(values)-1] }
func maxInt32(values []int32) int32 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
func minInt32(values []int32) int32 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
func sumInt32(values []int32) int32 {
	var sum int32
	for _, v := range values {
		sum += v
	}
	return sum
}

func firstInt64(values []int64) int64 { return values[0] }
func lastInt64(values []int64) int64  { return values[len(values)-1] }
func maxInt64(values []int64) int64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
func minInt64(values []int64) int64 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
func sumInt64(values []int64) int64 {
	var sum int64
	for _, v := range values {
		sum += v
	}
	return sum
}

func firstUint64(values []uint64) uint64 { return values[0] }
func lastUint64(values []uint64) uint64  { return values[len(values)-1] }
func maxUint64(values []uint64) uint64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}
func minUint64(values []uint64) uint64 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}
func sumUint64(values []uint64) uint64 {
	var sum uint64
	for _, v := range values {
		sum += v
	}
	return sum
}

var float32AccumFunc = map[string]interface{}{
	"first": firstFloat32,
	"max":   maxFloat32,
	"min":   minFloat32,
	"last":  lastFloat32,
	"sum":   sumFloat32,
}

var float64AccumFunc = map[string]interface{}{
	"first": firstFloat64,
	"max":   maxFloat64,
	"min":   minFloat64,
	"last":  lastFloat64,
	"sum":   sumFloat64,
}

var int32AccumFunc = map[string]interface{}{
	"first": firstInt32,
	"max":   maxInt32,
	"min":   minInt32,
	"last":  lastInt32,
	"sum":   sumInt32,
}

var int64AccumFunc = map[string]interface{}{
	"first": firstInt64,
	"max":   maxInt64,
	"min":   minInt64,
	"last":  lastInt64,
	"sum":   sumInt64,
}

var uint64AccumFunc = map[string]interface{}{
	"first": firstUint64,
	"max":   maxUint64,
	"min":   minUint64,
	"last":  lastUint64,
	"sum":   sumUint64,
}

var accumMap = map[io.EnumElementType]func(funcName string, column interface{}) *accumulator{
	io.FLOAT32: func(funcName string, column interface{}) *accumulator {
		ifunc := float32AccumFunc[funcName]
		iout := make([]float32, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.FLOAT64: func(funcName string, column interface{}) *accumulator {
		ifunc := float64AccumFunc[funcName]
		iout := make([]float64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.INT32: func(funcName string, column interface{}) *accumulator {
		ifunc := int32AccumFunc[funcName]
		iout := make([]int32, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.INT64: func(funcName string, column interface{}) *accumulator {
		ifunc := int64AccumFunc[funcName]
		iout := make([]int64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.UINT64: func(funcName string, column interface{}) *accumulator {
		ifunc := uint64AccumFunc[funcName]
		iout := make([]uint64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
}

func newAccumulator(cs *io.ColumnSeries, param accumParam) *accumulator {
	inColumn := cs.GetColumn(param.inputName)
	elemType := io.GetElementType(inColumn)
	if fn, ok := accumMap[elemType]; ok {
		return fn(param.funcName, inColumn)
	}
	// Return nil accumulator for unsupported types
	return &accumulator{}
}

func (ac *accumulator) apply(start, end int) error {
	if ac.ifunc == nil {
		return nil // Skip unsupported types
	}

	switch fn := ac.ifunc.(type) {
	case func([]float32) float32:
		return ac.float32out(fn, start, end)
	case func([]float64) float64:
		return ac.float64out(fn, start, end)
	case func([]int32) int32:
		return ac.int32out(fn, start, end)
	case func([]int64) int64:
		return ac.int64out(fn, start, end)
	case func([]uint64) uint64:
		return ac.uint64out(fn, start, end)
	default:
		return fmt.Errorf("unexpected ifunc type in accumulator: %T", ac.ifunc)
	}
}

func (ac *accumulator) float32out(fn func([]float32) float32, start, end int) error {
	ival, ok := ac.ivalues.([]float32)
	if !ok {
		return fmt.Errorf("expected []float32, got %T", ac.ivalues)
	}
	out := ac.iout.([]float32)
	out = append(out, fn(ival[start:end]))
	ac.iout = out
	return nil
}

func (ac *accumulator) float64out(fn func([]float64) float64, start, end int) error {
	ival, ok := ac.ivalues.([]float64)
	if !ok {
		return fmt.Errorf("expected []float64, got %T", ac.ivalues)
	}
	out := ac.iout.([]float64)
	out = append(out, fn(ival[start:end]))
	ac.iout = out
	return nil
}

func (ac *accumulator) int32out(fn func([]int32) int32, start, end int) error {
	ival, ok := ac.ivalues.([]int32)
	if !ok {
		return fmt.Errorf("expected []int32, got %T", ac.ivalues)
	}
	out := ac.iout.([]int32)
	out = append(out, fn(ival[start:end]))
	ac.iout = out
	return nil
}

func (ac *accumulator) int64out(fn func([]int64) int64, start, end int) error {
	ival, ok := ac.ivalues.([]int64)
	if !ok {
		return fmt.Errorf("expected []int64, got %T", ac.ivalues)
	}
	out := ac.iout.([]int64)
	out = append(out, fn(ival[start:end]))
	ac.iout = out
	return nil
}

func (ac *accumulator) uint64out(fn func([]uint64) uint64, start, end int) error {
	ival, ok := ac.ivalues.([]uint64)
	if !ok {
		return fmt.Errorf("expected []uint64, got %T", ac.ivalues)
	}
	out := ac.iout.([]uint64)
	out = append(out, fn(ival[start:end]))
	ac.iout = out
	return nil
}
