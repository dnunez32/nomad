package state

import (
	"fmt"

	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	TypeNodeRegistration         = "NodeRegistration"
	TypeNodeDeregistration       = "NodeDeregistration"
	TypeNodeEligibilityUpdate    = "NodeEligibility"
	TypeNodeDrain                = "NodeDrain"
	TypeNodeEvent                = "NodeEvent"
	TypeDeploymentUpdate         = "DeploymentStatusUpdate"
	TypeDeploymentPromotion      = "DeploymentPromotion"
	TypeDeploymentAllocHealth    = "DeploymentAllocHealth"
	TypeAllocCreated             = "AllocCreated"
	TypeAllocUpdated             = "AllocUpdated"
	TypeAllocUpdateDesiredStatus = "AllocUpdateDesiredStatus"
	TypeEvalUpdated              = "EvalUpdated"
	TypeJobRegistered            = "JobRegistered"
	TypeJobDeregistered          = "JobDeregistered"
	TypeJobBatchDeregistered     = "JobBatchDeregistered"
	TypePlanResult               = "PlanResult"
)

// JobEvent holds a newly updated Job.
type JobEvent struct {
	Job *structs.Job
}

// EvalEvent holds a newly updated Eval.
type EvalEvent struct {
	Eval *structs.Evaluation
}

// AllocEvent holds a newly updated Allocation. The
// Allocs embedded Job has been removed to reduce size.
type AllocEvent struct {
	Alloc *structs.Allocation
}

// DeploymentEvent holds a newly updated Deployment.
type DeploymentEvent struct {
	Deployment *structs.Deployment
}

// NodeEvent holds a newly updated Node
type NodeEvent struct {
	Node *structs.Node
}

// NNodeDrainEvent is the Payload for a NodeDrain event. It contains
// information related to the Node being drained as well as high level
// information about the current allocations on the Node
type NodeDrainEvent struct {
	Node      *structs.Node
	JobAllocs map[string]*JobDrainDetails
}

type NodeDrainAllocDetails struct {
	ID      string
	Migrate *structs.MigrateStrategy
}

type JobDrainDetails struct {
	Type         string
	AllocDetails map[string]NodeDrainAllocDetails
}

// GenericEventsFromChanges returns a set of events for a given set of
// transaction changes. It currently ignores Delete operations.
func GenericEventsFromChanges(tx ReadTxn, changes Changes) (*structs.Events, error) {
	var eventType string
	switch changes.MsgType {
	case structs.NodeRegisterRequestType:
		eventType = TypeNodeRegistration
	case structs.UpsertNodeEventsType:
		eventType = TypeNodeEvent
	case structs.EvalUpdateRequestType:
		eventType = TypeEvalUpdated
	case structs.AllocClientUpdateRequestType:
		eventType = TypeAllocUpdated
	case structs.JobRegisterRequestType:
		eventType = TypeJobRegistered
	case structs.AllocUpdateRequestType:
		eventType = TypeAllocUpdated
	case structs.NodeUpdateStatusRequestType:
		eventType = TypeNodeEvent
	case structs.JobDeregisterRequestType:
		eventType = TypeJobDeregistered
	case structs.JobBatchDeregisterRequestType:
		eventType = TypeJobBatchDeregistered
	case structs.AllocUpdateDesiredTransitionRequestType:
		eventType = TypeAllocUpdateDesiredStatus
	case structs.NodeUpdateEligibilityRequestType:
		eventType = TypeNodeDrain
	case structs.BatchNodeUpdateDrainRequestType:
		eventType = TypeNodeDrain
	case structs.DeploymentStatusUpdateRequestType:
		eventType = TypeDeploymentUpdate
	case structs.DeploymentPromoteRequestType:
		eventType = TypeDeploymentPromotion
	case structs.DeploymentAllocHealthRequestType:
		eventType = TypeDeploymentAllocHealth
	case structs.ApplyPlanResultsRequestType:
		eventType = TypePlanResult
	default:
		// unknown request type
		return nil, nil
	}

	var events []structs.Event
	for _, change := range changes.Changes {
		switch change.Table {
		case "evals":
			if change.Deleted() {
				return nil, nil
			}
			after, ok := change.After.(*structs.Evaluation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Evaluation")
			}

			event := structs.Event{
				Topic:     structs.TopicEval,
				Type:      eventType,
				Index:     changes.Index,
				Key:       after.ID,
				Namespace: after.Namespace,
				Payload: &EvalEvent{
					Eval: after,
				},
			}

			events = append(events, event)

		case "allocs":
			if change.Deleted() {
				return nil, nil
			}
			after, ok := change.After.(*structs.Allocation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Allocation")
			}

			alloc := after.Copy()

			filterKeys := []string{
				alloc.JobID,
				alloc.DeploymentID,
			}

			// remove job info to help keep size of alloc event down
			alloc.Job = nil

			event := structs.Event{
				Topic:      structs.TopicAlloc,
				Type:       eventType,
				Index:      changes.Index,
				Key:        after.ID,
				FilterKeys: filterKeys,
				Namespace:  after.Namespace,
				Payload: &AllocEvent{
					Alloc: alloc,
				},
			}

			events = append(events, event)
		case "jobs":
			if change.Deleted() {
				return nil, nil
			}
			after, ok := change.After.(*structs.Job)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Allocation")
			}

			event := structs.Event{
				Topic:     structs.TopicJob,
				Type:      eventType,
				Index:     changes.Index,
				Key:       after.ID,
				Namespace: after.Namespace,
				Payload: &JobEvent{
					Job: after,
				},
			}

			events = append(events, event)
		case "nodes":
			if change.Deleted() {
				return nil, nil
			}
			after, ok := change.After.(*structs.Node)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Node")
			}

			event := structs.Event{
				Topic: structs.TopicNode,
				Type:  eventType,
				Index: changes.Index,
				Key:   after.ID,
				Payload: &NodeEvent{
					Node: after,
				},
			}
			events = append(events, event)
		case "deployment":
			if change.Deleted() {
				return nil, nil
			}
			after, ok := change.After.(*structs.Deployment)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Node")
			}

			event := structs.Event{
				Topic:      structs.TopicDeployment,
				Type:       eventType,
				Index:      changes.Index,
				Key:        after.ID,
				Namespace:  after.Namespace,
				FilterKeys: []string{after.JobID},
				Payload: &DeploymentEvent{
					Deployment: after,
				},
			}
			events = append(events, event)
		}
	}

	return &structs.Events{Index: changes.Index, Events: events}, nil
}
