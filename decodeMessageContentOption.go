package redis

var _ DecodeMessageContentOption = DecodeMessageContentOptionFunc(nil)

type DecodeMessageContentOptionFunc func(setting *DecodeMessageContentSetting)

func (fn DecodeMessageContentOptionFunc) apply(setting *DecodeMessageContentSetting) {
	fn(setting)
}

// ------------------------------
func WithMessageStateKeyPrefix(prefix string) DecodeMessageContentOption {
	return DecodeMessageContentOptionFunc(func(setting *DecodeMessageContentSetting) {
		setting.MessageStateKeyPrefix = prefix
	})
}
