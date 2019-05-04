package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

//任务
type Job struct {
	Name     string `json:"name"`     //任务名称
	Command  string `json:"command"`  //任务命令行
	CronExpr string `json:"cronExpr"` //任务crontab表达式
}

//任务事件
type JobEvent struct {
	EventType int //任务类型，1 保存修改 2 删除
	Job       *Job
}

//任务执行时间计划
type JobEventPlan struct {
	Job      *Job
	Expr     *cronexpr.Expression //crontab时间格式
	NextTime time.Time            //下一次执行时间
}

//正在执行中的任务
type JobExecting struct {
	Job         *Job
	ExpectTime  time.Time          //期待执行时间
	RealityTime time.Time          //实际执行时间
	Ctx         context.Context    //用来做取消上下文
	CtxFunc     context.CancelFunc //用来做取消上下文函数
}

//任务执行结果
type JobExcuteResult struct {
	JobExecting *JobExecting
	OutPut      []byte    //内容输出
	Err         error     //错误内容
	StartTime   time.Time //工作开始时间
	EndTime     time.Time //工作结束时间
}

//任务日志
type JobLog struct {
	JobName     string `json:"jobName" bson:"jobName"`         //任务名称
	Command     string `json:"command" bson:"command"`         //脚本命令
	Err         string `json:"err" bson:"err"`                 //错误内容
	Output      string `json:"output" bson:"output"`           //输出内容
	ExpectTime  int64  `json:"expectTime" bson:"expectTime"`   //期待执行时间
	RealityTime int64  `json:"realityTime" bson:"realityTime"` //实际执行时间
	StartTime   int64  `json:"startTime" bson:"startTime"`     //工作开始时间
	EndTime     int64  `json:"endTime" bson:"endTime"`         //工作结束时间
}

//任务日志组
type JobLogBatch struct {
	Logs []interface{}
}

//任务日志查询过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

//任务日志查询排序
type JobLogSortByStartTime struct {
	StartTime int `bson:"startTime"` //{startTime:-1} 倒序
}

//统一返回结构格式
type Response struct {
	Errno int         `json:"errno"` //返回码 1代表正确，-1代表错误
	Msg   string      `json:"msg"`   //返回信息 错误的返回错误信息
	Data  interface{} `json:"data"`  //返回数据 错误返回nil
}

//构建返回格式基础(byte返回)
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)
	response = Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}
	//序列化为json返回
	resp, err = json.Marshal(response)
	return
}

//成功返回
func SuccessResponse(data interface{}) (resp []byte, err error) {
	resp, err = BuildResponse(1, "success", data)
	return
}

//失败返回
func FailResponse(errMsg string) (resp []byte, err error) {
	resp, err = BuildResponse(-1, errMsg, nil)
	return
}

//反序列化job
func Unserialize(data []byte) (job *Job, err error) {
	err = json.Unmarshal(data, &job)
	return
}

//获取完整job中job的名称
func GetExtraJobName(jobByte []byte) string {
	return strings.Trim(string(jobByte), JOB_SAVE_DIR)
}

//获取完整kill中job的名称
func GetExtraKillName(jobByte []byte) string {
	return strings.Trim(string(jobByte), JOB_KILLER_DIR)
}

//获取完整worker中job的名称
func GetExtraWorkerName(registerKeyByte []byte) string {
	return strings.Trim(string(registerKeyByte), JOB_WORKERS_DIR)
}

//任务事件变化有2中  1.更新任务 2.删除任务 3.终止任务
func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

//构建JobEventPlan
func BuildJobEventPlan(jobEvent *JobEvent) (plan *JobEventPlan, err error) {
	var (
		expr *cronexpr.Expression
	)
	if expr, err = cronexpr.Parse(jobEvent.Job.CronExpr); err != nil {
		return
	}
	plan = &JobEventPlan{
		Job:      jobEvent.Job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

//构建JobExecting
func BuildJobExecting(plan *JobEventPlan) *JobExecting {
	var (
		ctx     context.Context
		ctxFunc context.CancelFunc
	)
	ctx, ctxFunc = context.WithCancel(context.TODO())
	return &JobExecting{
		Job:         plan.Job,
		ExpectTime:  plan.NextTime,
		RealityTime: time.Now(),
		Ctx:         ctx,
		CtxFunc:     ctxFunc,
	}
}
