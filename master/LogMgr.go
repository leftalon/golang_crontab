package master

import (
	"context"
	"cron/common"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type logMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_LogMgr *logMgr
)

func InitLogLink() (err error) {
	var (
		client     *mongo.Client
		collection *mongo.Collection
	)
	if client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(G_config.MongoDbConfig.MongodbUri), options.Client().SetConnectTimeout(time.Duration(G_config.MongoDbConfig.MongodbConnectTimeout)*time.Millisecond)); err != nil {
		return
	}
	collection = client.Database(G_config.LogConfig.LogDatabase).Collection(G_config.LogConfig.LogCollection)
	G_LogMgr = &logMgr{
		client:        client,
		logCollection: collection,
	}
	return
}

//查看任务日志
func (logMgr *logMgr) ListLog(name string, skip int, limit int) (logs []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.JobLogSortByStartTime
		res     *mongo.Cursor
		jobLog  *common.JobLog
	)
	//为了方便验证，将logs给初始个数
	logs = make([]*common.JobLog, 0)

	//过滤条件
	filter = &common.JobLogFilter{JobName: name}
	//排序
	logSort = &common.JobLogSortByStartTime{StartTime: -1}

	if res, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Sort: logSort,
		Skip: func(skip int) *int64 {
			i := int64(skip)
			return &i
		}(skip),
		Limit: func(limit int) *int64 {
			i := int64(limit)
			return &i
		}(limit),
	}); err != nil {
		return
	}
	defer res.Close(context.TODO())
	for res.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		if err = res.Decode(jobLog); err == nil {
			//没有问题，就输出，有问题的就不管
			logs = append(logs, jobLog)
		}
	}
	return
}
