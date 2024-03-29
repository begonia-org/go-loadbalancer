package goloadbalancer

import "fmt"

type lcEndpointImpl struct {
	Endpoint
}

func (l *lcEndpointImpl) ActivateConnections() int {
	return l.Stats().GetActivateConns()
}
func (l *lcEndpointImpl) InactiveConnections() int {
	return l.Stats().GetIdleConns()
}

func NewLCEndpointImpl(endpoint Endpoint) LeastConnectionsEndpoint {
	return &lcEndpointImpl{Endpoint: endpoint}
}

type WLCEndpointImpl struct {
	*lcEndpointImpl
	weight int
}

func (w *WLCEndpointImpl) Weight() int {
	return w.weight
}
func NewWLCEndpointImpl(endpoint Endpoint, weight int) WeightLeastConnectionsEndpoint {
	return &WLCEndpointImpl{
		lcEndpointImpl: &lcEndpointImpl{Endpoint: endpoint},
		weight:         weight,
	}
}

type wrrEndpointImpl struct {
	Endpoint
	weight        int
	currentWeight int
}

func (w *wrrEndpointImpl) Weight() int {
	return w.weight
}

func (w *wrrEndpointImpl) CurrentWeight() int {
	return w.currentWeight
}
func (w *wrrEndpointImpl) SetWeight(weight int) {
	w.currentWeight = weight
}

func NewWRREndpointImpl(endpoint Endpoint, weight int) WeightEndpoint {
	return &wrrEndpointImpl{
		Endpoint:      endpoint,
		weight:        weight,
		currentWeight: weight,
	}
}

type sedEndpointImpl struct {
	Endpoint
	weight int
}

func (s *sedEndpointImpl) ActivateConnections() int {
	return s.Stats().GetActivateConns()
}
func (s *sedEndpointImpl) Weight() int {
	return s.weight
}

func NewSedEndpointImpl(endpoint Endpoint, weight int) ShortestExpectedDelayEndpoint {
	return &sedEndpointImpl{
		Endpoint: endpoint,
		weight:   weight,
	}
}

// const (
//
//	RRBalanceType             BalanceType = "RoundRobin"
//	WRRBalanceType            BalanceType = "WeightedRoundRobin"
//	ConsistentHashBalanceType BalanceType = "ConsistentHash"
//	LCBalanceType             BalanceType = "LeastConnection"
//	SEDBalanceType            BalanceType = "ShortestExpectedDelay"
//	WLCBalanceType            BalanceType = "WeightedLeastConnection"
//	NQBalanceType             BalanceType = "NeverQueue"
//
// )
func New(lbType BalanceType, endpoints []Endpoint) (LoadBalance, error) {
	switch lbType {
	case RRBalanceType:
		return NewRoundRobinBalance(endpoints), nil
	case WRRBalanceType:
		return NewWeightedRoundRobinBalance(endpoints), nil
	case ConsistentHashBalanceType:
		return NewConsistentHashBalancer(endpoints, 10), nil
	case LCBalanceType:
		return NewLeastConnectionsBalance(endpoints), nil
	case SEDBalanceType:
		return NewShortestExpectedDelayBalance(endpoints), nil
	case WLCBalanceType:
		return NewWeightLeastConnectionsBalance(endpoints), nil
	case NQBalanceType:
		return NewNeverQueueBalance(endpoints), nil
	default:
		return nil, fmt.Errorf("Unknown load balance type")
	}
}

// func NewEndpoints(lbType BalanceType,endpoints []EndpointMeta)([]Endpoint,error){
// 	switch lbType{
// 	case RRBalanceType:
// 		return NewRoundRobinBalance(endpoints),nil
// 	case WRRBalanceType:
// 		return NewWeightedRoundRobinBalance(endpoints),nil
// 	case ConsistentHashBalanceType:
// 		return NewConsistentHashBalancer(endpoints,10),nil
// 	case LCBalanceType:
// 		return NewLeastConnectionsBalance(endpoints),nil
// 	case SEDBalanceType:
// 		return NewShortestExpectedDelayBalance(endpoints),nil
// 	case WLCBalanceType:
// 		return NewWeightLeastConnectionsBalance(endpoints),nil
// 	case NQBalanceType:
// 		return NewNeverQueueBalance(endpoints),nil
// 	default:
// 		return nil,fmt.Errorf("Unknown load balance type")

// 	}
// }
