package worker

import (
	"context"
	"cron/common"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
)

//用来创建etcd服务
type jobMgr struct {
	Client  *clientv3.Client
	Kv      clientv3.KV
	Lease   clientv3.Lease
	Watcher clientv3.Watcher
}

var (
	G_jobMgr *jobMgr
)

func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	//通过配置文件来读取
	config = clientv3.Config{
		Endpoints:   G_config.EtcdConfig.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdConfig.EtcdDialTimeout) * time.Millisecond,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	G_jobMgr = &jobMgr{
		Client:  client,
		Kv:      kv,
		Lease:   lease,
		Watcher: watcher,
	}
	//启动监听新增编辑删除任务
	G_jobMgr.watchJob()

	//启动监听强杀任务
	G_jobMgr.watchKillJob()
	return
}

//监听任务
func (jobmgr *jobMgr) watchJob() (err error) {
	var (
		jobKey     string
		getResp    *clientv3.GetResponse
		kv         *mvccpb.KeyValue
		job        *common.Job
		revision   int64
		watch_chan clientv3.WatchChan
		watch_resp clientv3.WatchResponse
		event      *clientv3.Event
		key        string
		jobEvent   *common.JobEvent
	)
	jobKey = common.JOB_SAVE_DIR

	//1 初始化，将已有任务先推送到scheduler调度协程
	//1.1先获取到所有的任务
	if getResp, err = jobmgr.Kv.Get(context.TODO(), jobKey, clientv3.WithPrefix()); err != nil {
		return
	}
	//1.2将获取到的所有任务推送到scheduler(调度协程)
	for _, kv = range getResp.Kvs {
		//需要转json
		if job, err = common.Unserialize(kv.Value); err != nil {
			//无法序列化的就跳过
			err = nil
			continue
		}
		jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
		//把这个job同步给scheduler(调度协程)
		G_Scheduler.PushJobEventChan(jobEvent)
	}
	//2 打开一个协程，用来监听job任务变化，再将变化推送到scheduler调度协程
	go func() {
		//2.1 获取当前版本号
		revision = getResp.Header.Revision + 1
		//2.2开始监听
		watch_chan = jobmgr.Watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(revision), clientv3.WithPrefix())
		//watch_resp里有切片，无法直接比较空，只能用for...range ,不能用select
		for watch_resp = range watch_chan {
			//因为etcd在watch的时候，为了高吞吐量以及效率，所以有可能一次会发送多种监听事件在event中
			//然后存入chan
			for _, event = range watch_resp.Events {
				switch event.Type {
				case mvccpb.PUT:
					//保存任务，需要修改后的整体job
					if job, err = common.Unserialize(event.Kv.Value); err != nil {
						//有错误就跳过
						err = nil
						continue
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					//删除任务，所以只要key就好
					key = common.GetExtraJobName(event.Kv.Key)
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, &common.Job{
						Name: key,
					})
				}
				//将修改任务推入到scheduler调度协程
				//将删除任务推入到scheduler调度协程
				G_Scheduler.PushJobEventChan(jobEvent)
			}
		}
	}()

	return
}

//监听任务
func (jobmgr *jobMgr) watchKillJob() (err error) {
	var (
		jobKey     string
		getResp    *clientv3.GetResponse
		job        *common.Job
		revision   int64
		watch_chan clientv3.WatchChan
		watch_resp clientv3.WatchResponse
		event      *clientv3.Event
		jobEvent   *common.JobEvent
		key        string
	)
	//这里不需要查找之前要删除的key，因为都是一秒过期，可能还没启动就已经没了

	// 打开一个协程，用来监听job任务变化，再将变化推送到scheduler调度协程
	go func() {
		jobKey = common.JOB_KILLER_DIR

		//1 初始化，将已有任务先推送到scheduler调度协程
		//1.1先获取到所有的任务
		if getResp, err = jobmgr.Kv.Get(context.TODO(), jobKey); err != nil {
			return
		}
		//2.1 获取当前版本号
		revision = getResp.Header.Revision + 1
		//2.2开始监听
		watch_chan = jobmgr.Watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithRev(revision), clientv3.WithPrefix())
		//watch_resp里有切片，无法直接比较空，只能用for...range ,不能用select
		for watch_resp = range watch_chan {
			//因为etcd在watch的时候，为了高吞吐量以及效率，所以有可能一次会发送多种监听事件在event中
			//然后存入chan
			for _, event = range watch_resp.Events {
				switch event.Type {
				case mvccpb.PUT:
					//监听需要删除的key
					key = common.GetExtraKillName(event.Kv.Key)
					job = &common.Job{
						Name: key,
					}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					//推入到scheduler调度协程
					G_Scheduler.PushJobEventChan(jobEvent)
				case mvccpb.DELETE:
					//无需关心，这里触发的原因是删除的key任务名 1秒过期
				}
			}
		}
	}()

	return
}

//这个是jobMgr里的CreateJobLock
func (jobmgr *jobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	jobLock = CreateJobLock(jobName, jobmgr.Kv, jobmgr.Lease)
	return
}
