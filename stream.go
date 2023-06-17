package redis

var _ StreamOffsetInfo = Stream("")

type Stream string

func (s Stream) Offset(offset string) StreamOffset {
	return StreamOffset{
		Stream: string(s),
		Offset: offset,
	}
}

func (s Stream) Zero() StreamOffset {
	return StreamOffset{
		Stream: string(s),
		Offset: StreamZeroOffset,
	}
}

func (s Stream) NeverDeliveredOffset() StreamOffset {
	return StreamOffset{
		Stream: string(s),
		Offset: StreamNeverDeliveredOffset,
	}
}

// getStreamOffset implements StreamOffsetInfo.
func (s Stream) getStreamOffset() StreamOffset {
	return StreamOffset{
		Stream: string(s),
		Offset: StreamUnspecifiedOffset,
	}
}
