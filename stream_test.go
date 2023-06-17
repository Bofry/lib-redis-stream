package redis

import "testing"

func TestStream(t *testing.T) {
	{
		streamOffset := Stream("demo").NeverDeliveredOffset()

		var expectedStream string = "demo"
		if expectedStream != streamOffset.Stream {
			t.Errorf("StreamOffset.Stream expected:: %v, got:: %v", expectedStream, streamOffset.Stream)
		}
		var expectedOffset string = StreamNeverDeliveredOffset
		if expectedOffset != streamOffset.Offset {
			t.Errorf("StreamOffset.Offset expected:: %v, got:: %v", expectedOffset, streamOffset.Offset)
		}
	}
	{
		streamOffset := Stream("demo").Zero()

		var expectedStream string = "demo"
		if expectedStream != streamOffset.Stream {
			t.Errorf("StreamOffset.Stream expected:: %v, got:: %v", expectedStream, streamOffset.Stream)
		}
		var expectedOffset string = StreamZeroOffset
		if expectedOffset != streamOffset.Offset {
			t.Errorf("StreamOffset.Offset expected:: %v, got:: %v", expectedOffset, streamOffset.Offset)
		}
	}
	{
		streamOffset := Stream("demo").Offset("1000")

		var expectedStream string = "demo"
		if expectedStream != streamOffset.Stream {
			t.Errorf("StreamOffset.Stream expected:: %v, got:: %v", expectedStream, streamOffset.Stream)
		}
		var expectedOffset string = "1000"
		if expectedOffset != streamOffset.Offset {
			t.Errorf("StreamOffset.Offset expected:: %v, got:: %v", expectedOffset, streamOffset.Offset)
		}
	}
}
