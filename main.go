package main

import (
	"code.dream.com/logaent/common"
	"code.dream.com/logaent/etcd"
	"code.dream.com/logaent/kafka"
	"code.dream.com/logaent/tailfile"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

// 整个logaent的配置结构体
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}

type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

func run() {
	select {}
}

func main() {
	//-1:获取本机ip,为后续去etcd取配置文件做准备
	ip, err := common.GetOutboundIP()
	var configObj = new(Config)
	//var configObj Config
	if err != nil {
		logrus.Errorf("get ip failed,err:%v", err)
		return
	}

	//0、读配置文件 `go-ini`
	/*	cfg, err := ini.Load("./conf/config.ini")
		if err != nil {
			logrus.Error("load config failed,err:%v",err)
			return
		}
		kafkaAddr := cfg.Section("kafka").Key("address").String()
		fmt.Println(kafkaAddr)*/
	//err := ini.MapTo(&configObj, "./conf/config.ini")
	err = ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed,err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)
	//1、初始化连续kafka（做好准备工作）
	err = kafka.Init([]string{configObj.KafkaConfig.Address}, configObj.KafkaConfig.ChanSize)
	//err = kafka.Init(strings.Split(cfg.))
	if err != nil {
		logrus.Errorf("init kafka failed, err:%v", err)
		return
	}
	logrus.Info("init kafka success!")

	// 初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed,err:%v", err)
		return
	}
	// 从etcd中拉取要收集日志的配置项
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	//allConf, err := etcd.GetConf(configObj.EtcdConfig.CollectKey)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed, err:%v", err)
	}
	fmt.Println(allConf)
	// 派一个小弟去监控etcd中 configObj.EtcdConfig.CollectKey 对应值的变化
	//go etcd.WatchConf(configObj.EtcdConfig.CollectKey)
	go etcd.WatchConf(collectKey)
	//2、根据配置中的日志路径使用tail去收集日志
	//err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	err = tailfile.Init(allConf) // 把从etcd中获取的配置项传到Init中
	if err != nil {
		logrus.Error("init tailfile failed, err:%v", err)
		return
	}
	logrus.Info("init tailfile success!")
	run()
}
