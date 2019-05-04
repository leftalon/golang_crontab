package worker

import (
	"context"
	"cron/common"

	"go.etcd.io/etcd/clientv3"
)

type JobLock struct {
	jobName    string
	kv         clientv3.KV
	lease      clientv3.Lease
	cancelFunc context.CancelFunc
	leaseId    clientv3.LeaseID
	resFlag    bool
}

func CreateJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		jobName: jobName,
		kv:      kv,
		lease:   lease,
	}
}

//抢占锁
func (jobLock *JobLock) tryGetLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		ctx            context.Context
		ctxFunc        context.CancelFunc
		leaseKeepChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepResp       *clientv3.LeaseKeepAliveResponse
		txn            clientv3.Txn
		jobLockKey     string
		txnResp        *clientv3.TxnResponse
	)
	//1.创建租约，设置过期时间
	if leaseGrantResp, err = jobLock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}
	//2.创建上下文取消
	ctx, ctxFunc = context.WithCancel(context.TODO())
	//3.保持租约通讯
	if leaseKeepChan, err = jobLock.lease.KeepAlive(ctx, leaseGrantResp.ID); err != nil {
		goto ERR
	}
	//开启一个协程，进行监听续租结果
	go func() {
	breakSelect:
		for {
			select {
			case keepResp = <-leaseKeepChan:
				if keepResp == nil {
					//证明出了问题，不续租了
					break breakSelect
				}
			}
		}
	}()

	//4.抢锁-创建事务
	txn = jobLock.kv.Txn(context.TODO())
	//抢锁
	jobLockKey = common.JOB_LOCK_DIR + jobLock.jobName
	txn.If(clientv3.Compare(clientv3.CreateRevision(jobLockKey), "=", 0)).
		Then(clientv3.OpPut(jobLockKey, "", clientv3.WithLease(leaseGrantResp.ID))).
		Else(clientv3.OpGet(jobLockKey))
	//5.提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto ERR
	}
	//6.判断结果
	if !txnResp.Succeeded {
		//没有抢到锁
		err = common.ERROR_UNGET_LOCK
		goto ERR
	}

	//抢锁成功
	jobLock.resFlag = true
	jobLock.cancelFunc = ctxFunc
	jobLock.leaseId = leaseGrantResp.ID
	return
ERR:
	ctxFunc()                                               //停止自动续租
	jobLock.lease.Revoke(context.TODO(), leaseGrantResp.ID) //立刻释放租约
	return
}

//取消锁
func (jobLock *JobLock) unlock() {
	if jobLock.resFlag {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseId)
	}
}
