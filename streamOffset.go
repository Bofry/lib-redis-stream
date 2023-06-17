package redis

var _ StreamOffset = StreamOffset{}

type StreamOffset struct {
	Stream string
	Offset string
}

// getStreamOffset implements StreamOffsetInfo.
func (s StreamOffset) getStreamOffset() StreamOffset {
	return s
}
