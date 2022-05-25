package redis

import (
	"context"
	"encoding/json"

	"k8s.io/klog/v2"

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

// Use Redis data type - Set to store Logical Nodes
// One key has one record
//
// Note: Need re-visit these codes to see whether using function pointer is much better
func (gr *Goredis) PersistNodes(LogicalNodes []*types.LogicalNode) bool {
	for _, LogicalNode := range LogicalNodes {
		LogicalNodeKey := types.PreserveNode_KeyPrefix + LogicalNode.GetKey()
		LogicalNodeBytes, err := json.Marshal(LogicalNode)

		if err != nil {
			klog.Errorf("Error from JSON Marshal for Logical Nodes:", err)
			return false
		}

		err = gr.client.Set(gr.ctx, LogicalNodeKey, LogicalNodeBytes, 0).Err()

		if err != nil {
			klog.Errorf("Error to persist Logical Nodes to Redis Store:", err)
			return false
		}
	}

	return true
}

// Use Redis data type - String to store Node Store Status
//
func (gr *Goredis) PersistNodeStoreStatus(NodeStoreStatus *store.NodeStoreStatus) bool {
	NodeStoreStatusBytes, err := json.Marshal(NodeStoreStatus)

	if err != nil {
		klog.Errorf("Error from JSON Marshal for Node Store Status:", err)
		return false
	}

	err = gr.client.Set(gr.ctx, NodeStoreStatus.GetKey(), NodeStoreStatusBytes, 0).Err()

	if err != nil {
		klog.Errorf("Error to persist Node Store Status to Redis Store:", err)
		return false
	}

	return true
}

// Use Redis data type - String to store Virtual Node Assignment
//
func (gr *Goredis) PersistVirtualNodesAssignments(VirtualNodeAssignment *store.VirtualNodeAssignment) bool {
	VirtualNodeAssignmentBytes, err := json.Marshal(VirtualNodeAssignment)

	if err != nil {
		klog.Errorf("Error from JSON Marshal for Virtual Node Assignment:", err)
		return false
	}

	err = gr.client.Set(gr.ctx, VirtualNodeAssignment.GetKey(), VirtualNodeAssignmentBytes, 0).Err()

	if err != nil {
		klog.Errorf("Error to persist Virtual Node Assignment to Redis Store:", err)
		return false
	}

	return true
}

// Get all Logical Nodes based on PreserveNode_KeyPrefix = "MinNode"
//
//
// Note: Need re-visit these codes to see whether using function pointer is much better
func (gr *Goredis) GetNodes() []*types.LogicalNode {
	Keys := gr.client.Keys(gr.ctx, types.PreserveNode_KeyPrefix).Val()

	LogicalNodes := make([]*types.LogicalNode, len(Keys))

	var LogicalNode *types.LogicalNode

	for i, LogicalNodeKey := range Keys {
		value, err := gr.client.Get(gr.ctx, LogicalNodeKey).Result()

		if err != nil {
			klog.Errorf("Error to get LogicalNode from Redis Store:", err)
			return nil
		}

		if err != redis.Nil {
			bytes := []byte(value)
			err = json.Unmarshal(bytes, &LogicalNode)

			if err != nil {
				klog.Errorf("Error from JSON Unmarshal for LogicalNode:", err)
				return nil
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
		klog.Errorf("Error to get NodeStoreStatus from Redis Store:", err)
		return nil
	}

	if err != redis.Nil {
		bytes := []byte(value)
		err = json.Unmarshal(bytes, &NodeStoreStatus)

		if err != nil {
			klog.Errorf("Error from JSON Unmarshal for NodeStoreStatus:", err)
			return nil
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
		klog.Errorf("Error to get VirtualNodeAssignment from Redis Store:", err)
		return nil
	}

	if err != redis.Nil {
		bytes := []byte(value)
		err = json.Unmarshal(bytes, &VirtualNodeAssignment)

		if err != nil {
			klog.Errorf("Error from JSON Unmarshal for VirtualNodeAssignment:", err)
			return nil
		}
	}

	return VirtualNodeAssignment
}
