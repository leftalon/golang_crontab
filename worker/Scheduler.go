package worker

import (
	"cron/common"
	"fmt"
	"time"
)

//任务调度器

//将chan中的保存的jobEvent进行消费
//然后计算出下次执行时间，放在JobEventPlan'，内存保存
//然后每次遍历内存，找到要执行的任务即可
type Scheduler struct {
	JobEventChan     chan *common.JobEvent           //存放jobEvent
	JobEventPlan     map[string]*common.JobEventPlan //存放任务执行时间
	JobExectingTable map[string]*common.JobExecting  //存放正在执行中的任务
	JobExcuteResult  chan *common.JobExcuteResult    //存放任务执行的结果
}

var (
	G_Scheduler *Scheduler
)

//处理拿到的任务事件
//因为channel，所以不存在线程安全并发读写map
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		plan              *common.JobEventPlan
		err               error
		jobEventExists    bool
		jobExceting       *common.JobExecting
		jobExcetingExists bool
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE:
		//任务保存
		if plan, err = common.BuildJobEventPlan(jobEvent); err != nil {
			//任务格式不对，直接跳过
			return
		}
		scheduler.JobEventPlan[jobEvent.Job.Name] = plan
	case common.JOB_EVENT_DELETE:
		//任务删除
		//先检查任务是否存在
		if _, jobEventExists = scheduler.JobEventPlan[jobEvent.Job.Name]; jobEventExists {
			//存在就删除
			delete(scheduler.JobEventPlan, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL:
		//强杀任务
		if jobExceting, jobExcetingExists = scheduler.JobExectingTable[jobEvent.Job.Name]; jobExcetingExists {
			//证明这个command还在执行，可以进行强制退出
			jobExceting.CtxFunc()
		}
	}
}

//处理任务执行后的结果(删除正在执行表中的任务)
func (scheduler *Scheduler) handleJobResult(result *common.JobExcuteResult) {
	var (
		jobLog *common.JobLog
	)
	delete(scheduler.JobExectingTable, result.JobExecting.Job.Name)
	//编写日志
	if result.Err != common.ERROR_UNGET_LOCK {
		//如果是抢不到锁，这个日志就不需要记录了，否则就重复了(因为总有一个会去记录)
		jobLog = &common.JobLog{
			JobName:     result.JobExecting.Job.Name,
			Command:     result.JobExecting.Job.Command,
			Output:      string(result.OutPut),
			ExpectTime:  result.JobExecting.ExpectTime.UnixNano() / 1000 / 1000,  //取毫秒
			RealityTime: result.JobExecting.RealityTime.UnixNano() / 1000 / 1000, //取毫秒
			StartTime:   result.StartTime.UnixNano() / 1000 / 1000,               //取毫秒
			EndTime:     result.EndTime.UnixNano() / 1000 / 1000,                 //取毫秒
		}
		if result.Err != nil {
			//有错误时。进行记录
			jobLog.Err = result.Err.Error()
		} else {
			//没有错误就为空
			jobLog.Err = ""
		}
		//进行日志的记录
		G_LogLink.AppendLogLink(jobLog)
	}
	fmt.Println("任务完成:", result.JobExecting.Job.Name, string(result.OutPut), result.Err, result.StartTime, result.EndTime)
}

//将任务推进JobEventChan中
func (scheduler *Scheduler) PushJobEventChan(jobEvent *common.JobEvent) {
	scheduler.JobEventChan <- jobEvent
}

//尝试执行任务，因为同一个任务有可能有阻塞(比如自己规定自己要执行20秒)
func (scheduler *Scheduler) tryStartJob(plan *common.JobEventPlan) {
	var (
		jobDoing        bool
		jobExectingInfo *common.JobExecting
	)
	//调度和执行是两回事
	//执行的任务可能运行很久，1分钟会调度60次，但是只执行一次，防止并发
	//如果任务正在执行则跳过本次调度
	if _, jobDoing = scheduler.JobExectingTable[plan.Job.Name]; jobDoing {
		fmt.Println("当前任务正在执行:", plan.Job.Name, scheduler.JobExectingTable[plan.Job.Name])
		return
	}
	//否则加入，证明目前正在执行这个任务
	jobExectingInfo = common.BuildJobExecting(plan)
	scheduler.JobExectingTable[plan.Job.Name] = jobExectingInfo
	//开始执行任务
	G_Excutor.ExcuteJob(jobExectingInfo)
	//fmt.Println("开始执行任务:", jobExectingInfo.Job.Name, jobExectingInfo.ExpectTime, jobExectingInfo.RealityTime)
}

//遍历一遍map执行任务。同时返回计算出最近一次唤醒时间,这样定时器可以精确
func (scheduler *Scheduler) trySchedulerPlan() (schedulerAfter time.Duration) {
	var (
		jobEventPlan *common.JobEventPlan
		nextTime     *time.Time
		now          time.Time
	)
	if len(scheduler.JobEventPlan) == 0 {
		//没有任务则睡一秒
		schedulerAfter = 1 * time.Second
		return
	}
	//记录当前时间
	now = time.Now()
	for _, jobEventPlan = range scheduler.JobEventPlan {
		//执行任务
		if jobEventPlan.NextTime.Before(now) || jobEventPlan.NextTime.Equal(now) {
			//需要执行任务
			scheduler.tryStartJob(jobEventPlan)
			//更新时间
			//因为jobEventPlan是设置了地址传递，所以直接改变，不用去改变map
			jobEventPlan.NextTime = jobEventPlan.Expr.Next(now)
		}
		//当nextTime没有值或者jobEventPlan的nextTime在nextTime之前
		if nextTime == nil || jobEventPlan.NextTime.Before(*nextTime) {
			//把jobEventPlan的nextTime作为最近时间
			nextTime = &jobEventPlan.NextTime
		}
	}
	//相减获得秒数时间差
	schedulerAfter = nextTime.Sub(now)
	return
}

//消费chan
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent       *common.JobEvent
		schedulerAfter time.Duration
		schedulerTimer *time.Timer
		jobResult      *common.JobExcuteResult
	)

	//初始化的时候，就进行一次计算最近一次唤醒时间(按规定是1秒)
	schedulerAfter = scheduler.trySchedulerPlan()

	//设置一个定时器,在schedulerAfter设定的时间后执行一次
	schedulerTimer = time.NewTimer(schedulerAfter)
	for {
		//这里不会涉及到触发map的线程安全问题。因为是需要从上到下执行一遍，才会回到selec，所以不用担心
		select {
		case jobEvent = <-scheduler.JobEventChan:
			//处理获得到的任务事件
			scheduler.handleJobEvent(jobEvent)
		case <-schedulerTimer.C:
		// time.NewTimer到设定的时间后，schedulerTimer.C就会有一个值
		// 所以这里当<-scheduler.JobEventChan没有值的时候，就会一直阻塞，等待时间到
		// 不需要任何执行，因为到了这步，是由定时器到时间后执行的
		// 执行也是执行trySchedulerPlan
		case jobResult = <-scheduler.JobExcuteResult:
			//获取到任务执行结果
			//这步证明执行完毕，重新计算时间
			//因为有几率这个任务需要进行很久，不会立刻有结果
			//所以为了保证，多加一次结束后再重新进行一次计算时间
			scheduler.handleJobResult(jobResult)
		}
		//走到这里，证明要执行一次最近唤醒时间，重新计算，因为有新成员加入
		schedulerAfter = scheduler.trySchedulerPlan()
		//重新初始化一下定时器，让定时器继续开始
		schedulerTimer.Reset(schedulerAfter)
	}
}
func (scheduler *Scheduler) PushJobExcuteResult(result *common.JobExcuteResult) {
	scheduler.JobExcuteResult <- result
}

//初始化调度器，并且启动调度器进行chan消费
func InitScheduler() (err error) {
	G_Scheduler = &Scheduler{
		JobEventChan:     make(chan *common.JobEvent, 1024),
		JobEventPlan:     make(map[string]*common.JobEventPlan),
		JobExectingTable: make(map[string]*common.JobExecting),
		JobExcuteResult:  make(chan *common.JobExcuteResult, 1024),
	}
	go G_Scheduler.scheduleLoop()
	return
}
