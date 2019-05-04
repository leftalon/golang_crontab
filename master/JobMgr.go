package master

import (
	"context"
	"cron/common"
	"encoding/json"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
)

//用来创建etcd服务
type jobMgr struct {
	Client *clientv3.Client
	Kv     clientv3.KV
	Lease  clientv3.Lease
}

var (
	G_jobMgr *jobMgr
)

func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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
	G_jobMgr = &jobMgr{
		Client: client,
		Kv:     kv,
		Lease:  lease,
	}
	return
}

//保存job到etcd中，同时返回上一个被覆盖的值
// /cron/jobs/xxx => {"name":xxx,"command":xxx,"cronExpr":xxx}
func (jobMgr *jobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	var (
		jobKey   string
		jobValue []byte
		putResp  *clientv3.PutResponse
	)
	//1.etcd保存的key
	jobKey = common.JOB_SAVE_DIR + job.Name
	//2.将job进行序列化
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	//3.调用etcd的put方法，进行写入
	if putResp, err = jobMgr.Kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	//4.同时检查之前是否有值，返回之前的值
	if putResp.PrevKv != nil {
		//需要对旧的值进行反序列化处理
		//Unmarshal时，就算原来是指针，还是需要传入地址
		//因为这里的oldJob还只是一个指针，没有内容，也就是空指针
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJob); err != nil {
			err = nil //证明之前的value有问题，不过现在已经覆盖了，不算出错。
		}
	}
	return
}

//删除etcd中的job，同时返回上一个值
func (jobMgr *jobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey   string
		deleResp *clientv3.DeleteResponse
	)
	jobKey = common.JOB_SAVE_DIR + name
	if deleResp, err = jobMgr.Kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}
	//删除成功，获得之前的值
	//因为删除有Prefix的情况下会有多个值，所以这里的PrevKvs是一个切片，可以用len来判断
	if len(deleResp.PrevKvs) != 0 {
		//证明有删除东西]
		//Unmarshal时，就算原来是指针，还是需要传入地址
		//因为这里的oldJob还只是一个指针，没有内容，也就是空指针
		//极其危险，最好可以先赋值一下,这样也不需要传入地址了
		oldJob = &common.Job{}
		err = json.Unmarshal(deleResp.PrevKvs[0].Value, oldJob)
		if err != nil {
			//证明之前存入的格式不对，没关系
			err = nil
		}
	}
	//没有删除也没关系，直接返回即可
	return
}

//获取任务列表
func (jobMgr *jobMgr) ListJob() (joblist []*common.Job, err error) {
	var (
		jobKey  string
		getResp *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		job     *common.Job
	)
	//为了防止错乱，先给joblist初始化一下
	joblist = make([]*common.Job, 0)
	jobKey = common.JOB_SAVE_DIR
	//有内容不报错
	if getResp, err = jobMgr.Kv.Get(context.TODO(), jobKey, clientv3.WithPrefix()); err == nil {
		for _, kv = range getResp.Kvs {
			//json反序列化
			job = &common.Job{}
			if err = json.Unmarshal(kv.Value, job); err != nil {
				//如果反序列化过程中出错，就忽略跳过
				err = nil
				continue
			}
			joblist = append(joblist, job)
		}
	}
	return
}

//杀死任务。先将任务放到/cron/killer/中去，然后worker的监听会监听到这个变化，去执行关闭进程
func (jobMgr *jobMgr) KillJob(name string) (err error) {
	var (
		jobKey    string
		leaseResp *clientv3.LeaseGrantResponse
	)
	jobKey = common.JOB_KILLER_DIR + name
	//1.创建一个租约
	//原理是put要杀死的到/cron/killer/中去，worker的监听会监听到这个变化，去执行关闭进程。
	//这个key算是完成任务了,所以需要自动删除
	if leaseResp, err = jobMgr.Lease.Grant(context.TODO(), 1); err != nil {
		return
	}
	jobMgr.Kv.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseResp.ID))
	return err
}
