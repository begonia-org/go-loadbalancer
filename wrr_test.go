package goloadbalancer

import (
	"context"
	"testing"

	c "github.com/smartystreets/goconvey/convey"
)

type weightEndpointImpl struct {
	weight        int
	currentWeight int
}

func (w *weightEndpointImpl) Get(_ context.Context) (interface{}, error) {
	return nil, nil
}
func (w *weightEndpointImpl) Addr() string {
	return ""

}
func (w *weightEndpointImpl) Stats() Stats {
	return nil
}
func (w *weightEndpointImpl) Close() error {
	return nil
}
func (w *weightEndpointImpl) AfterTransform(ctx context.Context, cn Connection) {

}
func (w *weightEndpointImpl) Weight() int {
	return w.weight

}
func (w *weightEndpointImpl) CurrentWeight() int {
	return w.currentWeight
}
func (w *weightEndpointImpl) SetWeight(weight int) {
	w.currentWeight = weight

}
func newWeightEndpoint(weight int) *weightEndpointImpl {
	return &weightEndpointImpl{
		weight:        weight,
		currentWeight: weight,
	}
}
func TestWRR(t *testing.T) {
	c.Convey("TestWRR", t, func() {
		endpoints := make([]Endpoint, 0)
		endpoints = append(endpoints, newWeightEndpoint(4))
		endpoints = append(endpoints, newWeightEndpoint(2))
		endpoints = append(endpoints, newWeightEndpoint(1))
		expectEndpoint := []int{1, -1, 2, -2, 3, 0, 4}
		wrr := NewWeightedRoundRobinBalance(endpoints)
		for i := 0; i < 7; i++ {
			endpoint, err := wrr.Select()
			impl, _ := endpoint.(*weightEndpointImpl)
			c.So(err, c.ShouldBeNil)
			c.So(impl.CurrentWeight(), c.ShouldEqual, expectEndpoint[i])
			// t.Log(impl.currentWeight)
		}
	})
}
