package worker

import (
	"cron/common"
	"math/rand"
	"os/exec"
	"time"
)

//任务执行器
type Excutor struct {
}

var (
	G_Excutor *Excutor
)

//执行一个任务
func (excutor *Excutor) ExcuteJob(info *common.JobExecting) {
	go func() {
		var (
			cmd        *exec.Cmd
			res        []byte
			err        error
			jobExecRes *common.JobExcuteResult
			jobLock    *JobLock
		)
		jobExecRes = &common.JobExcuteResult{
			JobExecting: info,
			StartTime:   time.Now(),
		}
		//加入随机睡眠,让抢占更加公平
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		//加入分布式抢锁，否则多台机器同时运行，一个任务会被多台机器执行
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		//抢锁
		err = jobLock.tryGetLock()
		//内部用resflog做了判断，没有抢锁成功不会去再执行解除锁
		defer jobLock.unlock()
		if err != nil {
			//证明没有抢到锁
			jobExecRes.Err = err
			jobExecRes.EndTime = time.Now()
		} else {
			//抢到了锁
			jobExecRes.StartTime = time.Now()
			//终止运行中的任务需要这里的上下文
			cmd = exec.CommandContext(info.Ctx, "/bin/bash", "-c", info.Job.Command)
			//执行并且捕获输出
			res, err = cmd.CombinedOutput()
			jobExecRes.Err = err
			jobExecRes.OutPut = res
			jobExecRes.EndTime = time.Now()
		}
		G_Scheduler.PushJobExcuteResult(jobExecRes)
	}()
}

//初始化执行器
func InitExcutor() (err error) {
	G_Excutor = &Excutor{}
	return
}
