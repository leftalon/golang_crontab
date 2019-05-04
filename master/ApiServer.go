package master

import (
	"cron/common"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

type apiServer struct {
	httpServer *http.Server
}

var (
	//做全局的apiServer，等于单例
	G_apiServer *apiServer
)

//POST 提交
//job = {"name":"jobname","command":"echo 123","cronExpr":"* * * * *"}
//x-www-form-urlencoded
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postjob string
		job     common.Job
		oldjob  *common.Job
		resp    []byte
	)
	//1.解析post表单(go默认不会为我们做表单解析，因为需要消耗cpu，所以由我们手动操作)
	//将传过来的 a=1&b=2&c=3进行切割
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//2.获得表单里的值
	postjob = r.PostForm.Get("job")
	//3.反序列化为job(因为传过来的job对应的值已经被转为json格式)
	if err = json.Unmarshal([]byte(postjob), &job); err != nil {
		goto ERR
	}
	//4.将job传到etcd进行保存
	if oldjob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}
	//5.返回最后结果
	if resp, err = common.SuccessResponse(oldjob); err == nil {
		w.Write(resp)
	}
	return
ERR:
	//6.返回最后结果
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//POST 提交
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		resp   []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.PostForm.Get("name")
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	if resp, err = common.SuccessResponse(oldJob); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//GET 获取
func handleJobList(w http.ResponseWriter, r *http.Request) {
	//任务列表有哪些
	var (
		joblist []*common.Job
		err     error
		resp    []byte
	)
	if joblist, err = G_jobMgr.ListJob(); err != nil {
		goto ERR
	}
	if resp, err = common.SuccessResponse(joblist); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//POST 提交
//name = job1
func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		killName string
		err      error
		resp     []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	killName = r.PostForm.Get("name")
	if err = G_jobMgr.KillJob(killName); err != nil {
		goto ERR
	}
	if resp, err = common.SuccessResponse(nil); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//GET 获取
//查看任务日志
//name=一个测试123&skip=0&limit=10
func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		resp       []byte
		name       string
		skipParam  string
		limitParam string
		skip       int //offset 偏移量
		limit      int //分页大小
		logs       []*common.JobLog
	)
	//解析
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = G_config.LogConfig.LogDefaultLimitPage
	}
	if logs, err = G_LogMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}
	if resp, err = common.SuccessResponse(logs); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//服务注册发现列表(worker列表)
func handleWorkerList(w http.ResponseWriter, r *http.Request) {
	var (
		workers []string
		err     error
		resp    []byte
	)
	if workers, err = G_Worker.ListWorkers(); err != nil {
		goto ERR
	}
	if resp, err = common.SuccessResponse(workers); err == nil {
		w.Write(resp)
	}
	return
ERR:
	if resp, err = common.FailResponse(err.Error()); err == nil {
		w.Write(resp)
	}
	return
}

//初始化API HTTP服务
func InitApiServer() (err error) {
	var (
		mux          *http.ServeMux
		listener     net.Listener
		httpServer   *http.Server
		staticDir    http.Dir
		staticHandle http.Handler
	)
	//配置路由
	mux = http.NewServeMux()
	//给路由绑定闭包执行函数
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)
	//绑定静态页面
	//ServeMux遵循最大匹配原则，一个url进来，会将所有路由扫描一遍，只匹配最接近的

	//绑定静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	//制作handle
	staticHandle = http.FileServer(staticDir)
	//因为路由匹配上后，会将/index.html开头的/去掉，所以这里需要加回 "/" .
	// 这样才能完整找到 ./webroot/index.html 文件
	mux.Handle("/", http.StripPrefix("/", staticHandle))

	//路由跟函数绑定成功后，开启监听端口
	if listener, err = net.Listen("tcp", fmt.Sprintf(":%d", G_config.HttpConfig.ApiPort)); err != nil {
		return
	}
	//创建一个http服务的实例
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.HttpConfig.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.HttpConfig.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux, //绑定路由与对应的闭包函数
	}

	//赋值全局变量，作为单例
	G_apiServer = &apiServer{
		httpServer: httpServer,
	}
	//启动服务(因为服务是阻塞的，所以用协程启动)
	go httpServer.Serve(listener)
	return
}
