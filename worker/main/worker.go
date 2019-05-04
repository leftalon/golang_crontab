package main

import (
	"cron/worker"
	"flag"
	"fmt"
	"runtime"
	"time"
)

//总流程
//1. JobMgr先从etcd中拿出master中保存的任务，附带其属于的类型，将其推送到Scheduler中
//2. JobMgr再监听etcd中key的变化，将变化的以及对应的附带其属于的类型，将其推送到Scheduler中
//3. Scheduler先将传入的任务分别一个一个取出，执行一次后，计算其下一次执行的时间，将下一次执行的时间以及任务，放在JobEventPlan(放内存中)(属于调度)
//按照规则重复执行
//4. Scheduler执行任务时，再统计其期望执行时间与实际执行时间，放到JobExectingTable中去，作为记录目前正在执行的任务或者已经执行完毕的任务
//5.

//初始化线程
func initEnv() {
	//设置当前go程序的线程数最大为本机的cpu核数
	runtime.GOMAXPROCS(runtime.NumCPU())
}

//设置一个全局的配置文件路径，从命令行中获得
var ConfigPath string

//从命令行中读取参数，获得配置文件路径
func initArg() {
	flag.StringVar(&ConfigPath, "config", "./worker.json", "配置文件路径")
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
	if err = worker.InitConfig(ConfigPath); err != nil {
		goto ERR
	}
	//服务注册
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}

	//启动日志
	if err = worker.InitLogLink(); err != nil {
		goto ERR
	}

	//启动执行器
	if err = worker.InitExcutor(); err != nil {
		goto ERR
	}
	//启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	//启动任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	for {
		time.Sleep(1 * time.Second)
	}
	return
ERR:
	fmt.Println(err)
}
