package redis

var _ MessageDelegate = new(clientMessageDelegate)

type clientMessageDelegate struct {
	client *Consumer
}

// OnAck implements MessageDelegate.
func (d *clientMessageDelegate) OnAck(msg *Message) {
	d.client.doAck(msg)
}

// OnDel implements MessageDelegate.
func (d *clientMessageDelegate) OnDel(msg *Message) {
	d.client.doDel(msg)
}
