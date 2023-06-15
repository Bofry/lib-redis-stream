package redis

var _ ProduceMessageOption = new(ProduceMessageIDOption)

type ProduceMessageIDOption func(id string) string

func (proc ProduceMessageIDOption) applyContent(msg *MessageContent) error {
	return nil
}
func (proc ProduceMessageIDOption) applyID(id string) string {
	return proc(id)
}

func WithMessageID(id string) ProduceMessageIDOption {
	return func(string) string {
		return id
	}
}
