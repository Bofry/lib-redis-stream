package redis

type ForwarderRunner struct {
	handle *Forwarder
}

func (r *ForwarderRunner) Start() {
	defaultLogger.Println("Started")
}

func (r *ForwarderRunner) Stop() {
	defaultLogger.Println("Stopping")
	r.handle.Close()
	defaultLogger.Println("Stopped")
}
