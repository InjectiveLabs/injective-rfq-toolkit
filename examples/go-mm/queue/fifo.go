package queue

type FIFOSet struct {
	seen  map[string]struct{}
	order []string
	limit int
}

func NewFIFOSet(limit int) *FIFOSet {
	return &FIFOSet{
		seen:  make(map[string]struct{}, limit),
		order: make([]string, 0, limit),
		limit: limit,
	}
}

// Add returns true if the key was newly added.
// It returns false if the key was already present.
func (s *FIFOSet) Add(key string) bool {
	if _, ok := s.seen[key]; ok {
		return false
	}

	s.seen[key] = struct{}{}
	s.order = append(s.order, key)

	if len(s.order) > s.limit {
		oldest := s.order[0]
		delete(s.seen, oldest)

		// Remove first item from queue.
		copy(s.order, s.order[1:])
		s.order = s.order[:len(s.order)-1]
	}

	return true
}
