package main

import (
	"cron/master"
	"flag"
	"fmt"
	"runtime"
	"time"
)

//初始化线程
func initEnv() {
	//设置当前go程序的线程数最大为本机的cpu核数
	runtime.GOMAXPROCS(runtime.NumCPU())
}

//设置一个全局的配置文件路径，从命令行中获得
var ConfigPath string

//从命令行中读取参数，获得配置文件路径
func initArg() {
	flag.StringVar(&ConfigPath, "config", "./master.json", "配置文件路径")
	flag.Parse()
}
func main() {
	var (
		err error
	)
	//初始化线程
	initEnv()

	//读取命令行参数
	initArg()

	//加载配置文件
	if err = master.InitConfig(ConfigPath); err != nil {
		goto ERR
	}

	//启动etcd(连接etcd)
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}
	//服务注册发现查看
	if err = master.InitWorker(); err != nil {
		goto ERR
	}

	//启动日志(连接mongodb)
	if err = master.InitLogLink(); err != nil {
		goto ERR
	}
	//启动Api HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}
