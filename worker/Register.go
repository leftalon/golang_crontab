package worker

import (
	"context"
	"cron/common"
	"net"
	"time"

	"go.etcd.io/etcd/clientv3"
)

//服务注册
type register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIp string
}

var (
	G_Register *register
)

//获取本机ipv4，非环形地址
func getLocalIp() (ipv4 string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet
		isIpNet bool
	)

	//证明本机没有网卡，包括虚拟网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}
	//因为网卡可能有真实网卡或者是虚拟网卡(本地回环)
	for _, addr = range addrs {
		//因为addr中可能还包含了unixsocket的地址之类的.此时isIpNet就是false
		//并且去掉本地回环地址
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			//再判断是ipv4还是ipv6
			if ipNet.IP.To4() != nil {
				//如果是ipv6，则返回是个nil
				ipv4 = ipNet.IP.String() //192.168.1.1
				//找到一个就行
				return
			}
		}
	}
	err = common.ERROR_IP_NOT_FOUND
	return
}

//注册到/cron/workers/ip，并且自动续约
func (register *register) keepOnline() {
	var (
		registerKey       string
		leaseGrantResp    *clientv3.LeaseGrantResponse
		err               error
		ctx               context.Context
		ctxFunc           context.CancelFunc
		leaseKeepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		leaseKeep         *clientv3.LeaseKeepAliveResponse
	)
	registerKey = common.JOB_WORKERS_DIR + register.localIp

	for {
		ctxFunc = nil
		//1.生成租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto ReTry
		}
		//生成上下文
		ctx, ctxFunc = context.WithCancel(context.TODO())
		//自动续租
		if leaseKeepRespChan, err = register.lease.KeepAlive(ctx, leaseGrantResp.ID); err != nil {
			goto ReTry
		}
		//注册到etcd
		if _, err = register.kv.Put(context.TODO(), registerKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto ReTry
		}

		for {
			select {
			case leaseKeep = <-leaseKeepRespChan:
				if leaseKeep == nil {
					//证明此时连接异常，需要重新连接
					goto ReTry
				}
			}
		}
	ReTry:
		//取消租约
		if ctxFunc != nil {
			//因为一开始的时候没有生成ctxFunc，所以给一个nil来判断是哪里出的错误
			//将租约取消并且过期
			ctxFunc()
			register.lease.Revoke(context.TODO(), leaseGrantResp.ID)
		}
		//如果失败了，就过一秒再试能否上线
		time.Sleep(1 * time.Second)
	}

}

func InitRegister() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
		ipv4   string
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdConfig.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdConfig.EtcdDialTimeout) * time.Millisecond,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	if ipv4, err = getLocalIp(); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	G_Register = &register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIp: ipv4,
	}
	go G_Register.keepOnline()
	return
}
