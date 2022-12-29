// @Author huzejun 2022/12/21 0:58:00
package common

import (
	"fmt"
	"net"
	"strings"
)

// CollectEntry 要收集的日志的配置项结构体
type CollectEntry struct {
	Path  string `json:"path"`  //去哪个路径读取日志文件
	Topic string `json:"topic"` // 日志文件发送kafka中的那个 topic
}

// get preferred outbound ip of this machine
// CanNotGetIP  获取本机IP的函数
func GetOutboundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		//log.Fatal(err)
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	fmt.Println(localAddr.String())
	ip = strings.Split(localAddr.IP.String(), ":")[0]
	return
}
