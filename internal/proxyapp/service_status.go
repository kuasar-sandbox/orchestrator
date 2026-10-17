package proxyapp

// Public snapshots own their nested slices and pointers, including snapshots
// relayed to B. A caller may retain or modify a returned value safely.
func copyStatus(source Status) Status {
	result := source
	result.Reasons = append([]string{}, source.Reasons...)
	result.Instances = append([]InstanceStatus{}, source.Instances...)
	for index := range result.Instances {
		result.Instances[index].Reasons = append([]string{}, source.Instances[index].Reasons...)
	}
	if source.InplaceRestart != nil {
		operation := *source.InplaceRestart
		if operation.NextAttempt != nil {
			stamp := *operation.NextAttempt
			operation.NextAttempt = &stamp
		}
		result.InplaceRestart = &operation
	}
	return result
}

// Publish on state changes, taking change channels before reading the snapshot.
// A concurrent change therefore closes an observed channel instead of being lost.
func (s *Service) publishService(done <-chan struct{}) {
	for {
		s.mu.Lock()
		changed, local, child := s.changed, s.local, s.child
		s.mu.Unlock()
		var localChanged, childChanged <-chan struct{}
		if local != nil {
			local.mu.Lock()
			localChanged = local.changed
			local.mu.Unlock()
		}
		if child != nil {
			child.mu.Lock()
			childChanged = child.changed
			child.mu.Unlock()
			snapshot := s.Status()
			_ = child.wire.send(serviceMessage{Kind: "service", Service: &snapshot})
		}
		select {
		case <-changed:
		case <-localChanged:
		case <-childChanged:
		case <-done:
			return
		}
	}
}
