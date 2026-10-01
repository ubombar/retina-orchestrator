package orchestrator

type agentHeapNode struct {
	agentID string
	heap    schedulerHeap
	next    *agentHeapNode
}

func pushAgent(list **agentHeapNode, node *agentHeapNode) { node.next = *list; *list = node }

func popAgent(list **agentHeapNode, node *agentHeapNode) bool {
	for current := list; *current != nil; current = &(*current).next {
		if *current == node {
			*current = node.next
			node.next = nil
			return true
		}
	}
	return false
}
