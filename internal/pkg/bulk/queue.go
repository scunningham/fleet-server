package bulk

type queueT struct {
	ty      queueType
	head    *bulkT
	pending int
}

type queueType int

const (
	kQueueBulk queueType = iota
	kQueueRead
	kQueueSearch
	kQueueRefreshBulk
	kQueueRefreshRead
	kNumQueues
)

func (q queueT) Type() string {
	switch q.ty {
	case kQueueBulk:
		return "bulk"
	case kQueueRead:
		return "read"
	case kQueueSearch:
		return "search"
	case kQueueRefreshBulk:
		return "refreshBulk"
	case kQueueRefreshRead:
		return "refreshRead"
	}
	panic("unknown")
}
