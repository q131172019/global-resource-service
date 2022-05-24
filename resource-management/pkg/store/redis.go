package goredis

import (
	"context"
	"encoding/json"
	"fmt"

	"global-resource-service/resource-management/pkg/common-lib/interfaces/store"
	"global-resource-service/resource-management/pkg/common-lib/types"

	"github.com/go-redis/redis/v8"
)

type Goredis struct {
	client *redis.Client
	ctx    context.Context
}

// Initialize Redis Client
//
func NewRedisClient() *Goredis {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", //no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()

	return &Goredis{
		client: client,
		ctx:    ctx,
	}
}

const (
	PreserveNode_KeyPrefix = "MinNode"
)

// Use Redis data type - Set to store Logical Nodes
// One key has one record
//
func (gr *Goredis) PersistNodes(LogicalNodes []*types.LogicalNode) bool {
	for _, LogicalNode := range LogicalNodes {
		LogicalNodeKey := PreserveNode_KeyPrefix + LogicalNode.GetKey()
		LogicalNodeBytes, err := json.Marshal(LogicalNode)

		if err != nil {
			panic(err)
		}

		err = gr.client.Set(gr.ctx, LogicalNodeKey, LogicalNodeBytes, 0).Err()

		if err != nil {
			panic(err)
		}
	}

	return true
}

// Use Redis data type - String to store Node Store Status
//
func (gr *Goredis) PersistNodeStoreStatus(NodeStoreStatus *store.NodeStoreStatus) bool {
	NodeStoreStatusBytes, err := json.Marshal(NodeStoreStatus)

	if err != nil {
		panic(err)
	}

	err = gr.client.Set(gr.ctx, NodeStoreStatus.GetKey(), NodeStoreStatusBytes, 0).Err()

	if err != nil {
		panic(err)
	}

	return true
}

// Use Redis data type - String to store Virtual Node Assignment
//
func (gr *Goredis) PersistVirtualNodesAssignments(VirtualNodeAssignment *store.VirtualNodeAssignment) bool {
	VirtualNodeAssignmentBytes, err := json.Marshal(VirtualNodeAssignment)

	if err != nil {
		panic(err)
	}

	err = gr.client.Set(gr.ctx, VirtualNodeAssignment.GetKey(), VirtualNodeAssignmentBytes, 0).Err()

	if err != nil {
		panic(err)
	}

	return true
}

// Get all Logical Nodes based on PreserveNode_KeyPrefix = "MinNode"
//
func (gr *Goredis) GetNodes() []*types.LogicalNode {
	Keys := gr.client.Keys(gr.ctx, PreserveNode_KeyPrefix).Val()

	LogicalNodes := make([]*types.LogicalNode, len(Keys))

	var LogicalNode *types.LogicalNode

	for i, LogicalNodeKey := range Keys {
		value, err := gr.client.Get(gr.ctx, LogicalNodeKey).Result()

		if err != nil {
			panic(err)
		}

		if err != redis.Nil {
			bytes := []byte(value)
			err = json.Unmarshal(bytes, &LogicalNode)

			if err != nil {
				fmt.Println("Error from JSON Unmarshal for LogicalNode:", err)
			}

			LogicalNodes[i] = LogicalNode

		}
	}

	return LogicalNodes
}

// Get Node Store Status
//
func (gr *Goredis) GetNodeStoreStatus() *store.NodeStoreStatus {
	var NodeStoreStatus *store.NodeStoreStatus

	value, err := gr.client.Get(gr.ctx, NodeStoreStatus.GetKey()).Result()

	if err != nil {
		panic(err)
	}

	if err != redis.Nil {
		b := []byte(value)
		err = json.Unmarshal(b, &NodeStoreStatus)

		if err != nil {
			fmt.Println("Error from JSON Unmarshal for NodeStoreStatus:", err)
		}
	}

	return NodeStoreStatus
}

// Get Virtual Nodes Assignments
//
func (gr *Goredis) GetVirtualNodesAssignments() *store.VirtualNodeAssignment {
	var VirtualNodeAssignment *store.VirtualNodeAssignment

	value, err := gr.client.Get(gr.ctx, VirtualNodeAssignment.GetKey()).Result()

	if err != nil {
		panic(err)
	}

	if err != redis.Nil {
		b := []byte(value)
		err = json.Unmarshal(b, &VirtualNodeAssignment)

		if err != nil {
			fmt.Println("Error from JSON Unmarshal for VirtualNodeAssignment:", err)
		}
	}

	return VirtualNodeAssignment
}
