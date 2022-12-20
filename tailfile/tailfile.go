package tailfile

import (
	"code.dream.com/logaent/common"
	"code.dream.com/logaent/kafka"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

// tail相关
type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
}

func newTailTask(path, topic string) *tailTask {

	tt := &tailTask{
		path:  path,
		topic: topic,
	}
	return tt
}

func (t *tailTask) run() {
	// 读取日志，发送kafka
	logrus.Infof("collect for path:%s is running...", t.path)
	for {
		// 循环读数据
		line, ok := <-t.tObj.Lines // chan tail.Line
		if !ok {
			logrus.Warn("tail file close reopen, path:%s\n", t.path)
			time.Sleep(time.Second) // 读取出错等一秒
			continue
		}
		// 如果是空行就略过
		//fmt.Printf("%#v\n",line.Text)
		if len(strings.Trim(line.Text, "\r")) == 0 {
			logrus.Info("出现空行了，直接跳过！")
			continue
		}
		//利用通道将同步的代码改为异步的
		//把读出来的一行日志包装为kafka里面的msg类型
		msg := &sarama.ProducerMessage{}
		msg.Topic = t.topic //每个tailObj自己的topic
		msg.Value = sarama.StringEncoder(line.Text)
		//丢到通道中
		kafka.ToMsgChan(msg)
	}
}

func (t *tailTask) Init() (err error) {
	cfg := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	t.tObj, err = tail.TailFile(t.path, cfg)
	return
}

func Init(allConf []common.CollectEntry) (err error) {
	// allConf里面存了若干个日志的收集项
	// 针对每一个日志收集项创建一个对应的tailObj
	for _, conf := range allConf {
		tt := newTailTask(conf.Path, conf.Topic) //创建一个日志收集的任务
		err = tt.Init()                          // 去打开日志文件准备读
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v", conf.Path, err)
			continue
		}
		logrus.Infof("create a tail task for path:%s success", conf.Path)
		// 启动一个后台的goroutine去收集日志
		go tt.run()
	}
	return
}
