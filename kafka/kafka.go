// @Author huzejun 2022/12/18 21:17:00
package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// kafka相关操作
var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
	log     *logrus.Logger
)

/*type Message struct {
	Data string
	Topic string
}
*/

/*func init()  {
	log = logrus.New()
	//设置日志转出为os.Stdout
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	log.Info("kafka:init log success")

}
*/

// Init 是初始化全局的kafka Client
func Init(address []string, chanSize int64) (err error) {
	// 1.生产都配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // ACK
	config.Producer.Partitioner = sarama.NewRandomPartitioner //分区
	config.Producer.Return.Successes = true                   //确认

	//2、连接kafka	// 本机 192.168.1.111
	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("kafka: producer closed, err:", err)
		return
	}
	// 初始化
	msgChan = make(chan *sarama.ProducerMessage, chanSize)
	go sendMsg()
	return
}

// 从MsgChan中读取msg,发送给kafka
func sendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed, err:", err)
				return
			}
			logrus.Infof("send msg kafka success.pid:%v,offset:%v", pid, offset)
		}
	}
}

// 定义一个函数向外暴露msgChan
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg

}

// SendLog
/*func SendLog(msg *Message)(err error)  {
	select {
	case msgChan <- msg:
	default:
		err = fmt.Errorf("msgChan is full")
	}
	return
}*/

/*func sendKafka()  {
	for msg := range msgChan {
		kafkaMsg := &sarama.ProducerMessage{}
		kafkaMsg.Topic = msg.Topic
		kafkaMsg.Value = sarama.StringEncoder(msg.Data)
		pid, offset, err := client.SendMessage(kafkaMsg)
		if err != nil {
			log.Warnf("send msg failed, err:%v\n",err)
			continue
		}
		log.Infof("send msg success, pid:%v offset:%v\n",pid,offset)
	}
}*/
