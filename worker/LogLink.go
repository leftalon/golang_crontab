package worker

import (
	"context"
	"cron/common"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/mongo"
)

type logLink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.JobLogBatch
}

var (
	G_LogLink *logLink
)

func (logLink *logLink) saveLogs(batch *common.JobLogBatch) {
	//因为是插入日志，所以成功失败皆可
	logLink.logCollection.InsertMany(context.TODO(), batch.Logs)
}
func (logLink *logLink) writeLogLoop() {
	var (
		log          *common.JobLog
		batch        *common.JobLogBatch //当前的日志批次
		commitTimer  *time.Timer
		timeoutBatch *common.JobLogBatch //超时批次
	)
	for {
		select {
		case log = <-logLink.logChan:
			//将获得到的日志进行写入
			if batch == nil {
				//证明是已经提交了或者是刚刚进入
				batch = &common.JobLogBatch{}
				//超过规定阈值，进行自动提交
				commitTimer = time.AfterFunc(time.Duration(G_config.LogConfig.LogAutoCommitTimeOut)*time.Millisecond, func(batch *common.JobLogBatch) func() {
					//这里传入的batch会与外部的batch不相同
					return func() {
						//将传入的超时批次放到autoCommitChan，让select检索到去做后面的事情
						logLink.autoCommitChan <- batch
					}
				}(batch))

			}
			batch.Logs = append(batch.Logs, log)
			//查看目前数量是否达到阈值，达到就提交
			if len(batch.Logs) >= G_config.LogConfig.LogBranch {
				//提交
				logLink.saveLogs(batch)
				//清空
				batch = nil
				//已经自动提交了，就将定时器停止(取消)
				commitTimer.Stop()
			}
		case timeoutBatch = <-logLink.autoCommitChan:
			//超时批次
			//因为有可能刚发过来，日志马上满了。已经提交过了,batch就变化了
			if timeoutBatch != batch {
				continue //跳过提交
			}
			//提交
			logLink.saveLogs(timeoutBatch)
			//清空
			batch = nil
		}
	}
}

func InitLogLink() (err error) {
	var (
		client     *mongo.Client
		collection *mongo.Collection
	)
	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(G_config.MongoDbConfig.MongodbUri), options.Client().SetConnectTimeout(time.Duration(G_config.MongoDbConfig.MongodbConnectTimeout)*time.Millisecond)); err != nil {
		return
	}
	collection = client.Database(G_config.LogConfig.LogDatabase).Collection(G_config.LogConfig.LogCollection)
	G_LogLink = &logLink{
		client:         client,
		logCollection:  collection,
		logChan:        make(chan *common.JobLog, 1024),
		autoCommitChan: make(chan *common.JobLogBatch, 1024),
	}
	//启动一个协程进行监听是否有日志需要写入
	go G_LogLink.writeLogLoop()
	return
}
func (logLink *logLink) AppendLogLink(jobLog *common.JobLog) {
	//因为日志有可能很多，这样会阻塞，所以当阻塞时，把后面来的都丢掉
	//logLink.logChan<-jobLog
	select {
	case logLink.logChan <- jobLog:
	default:
		//队列已满，还有的直接丢弃
	}
}
