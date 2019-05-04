package master

import (
	"context"
	"cron/common"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
)

//服务注册
type worker struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIp string
}

var (
	G_Worker *worker
)

func InitWorker() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdConfig.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdConfig.EtcdDialTimeout) * time.Millisecond,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	G_Worker = &worker{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}
func (worker *worker) ListWorkers() (workers []string, err error) {
	var (
		getResp *clientv3.GetResponse
		kv      *mvccpb.KeyValue
		name    string
	)
	workers = make([]string, 0)
	if getResp, err = worker.kv.Get(context.TODO(), common.JOB_WORKERS_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kv = range getResp.Kvs {
		name = common.GetExtraWorkerName(kv.Key)
		workers = append(workers, name)
	}
	return
}
