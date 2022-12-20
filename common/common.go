// @Author huzejun 2022/12/21 0:58:00
package common

// CollectEntry 要收集的日志的配置项结构体
type CollectEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}
