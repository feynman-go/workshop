package node

import (
	"context"
	"github.com/feynman-go/workshop/cap/balance"
)

type RingCluster struct {
	ring *balance.Ring
}

func (cluster *RingCluster) run(ctx context.Context) {

}

type Node struct {
	ID int64
}

type Store interface {
	AddNodes(ctx context.Context, ids []int64) ([]Node, error)
	RemoveNodes(ctx context.Context, ids []Node) error
	GetNodes(ctx context.Context) ([]Node, error)
}

const (
	NodeTypeAddNode = 1
	NodeTypeRemoveNode = 2
	NodeTypeNodeUpdate = 3
)

type Message struct {
	Type int
	NodeID int64
	AvailableScore float64
}
