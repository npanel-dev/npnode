package cmd

import (
	"context"
	"time"

	"github.com/npanel-dev/npnode/common/logreport"
	log "github.com/sirupsen/logrus"
)

// fatalFlushTimeout 致命退出前尽力上报的最长时间
const fatalFlushTimeout = 5 * time.Second

// replayTimeout 启动时补发上次未送达报告的最长时间（ReplaySpool 发送期间持有 spool 锁，必须有界）
const replayTimeout = 5 * time.Second

// fatalExit 尽力把待发日志送出（送不出则落盘），然后以退出码 1 结束进程。
// 生产代码传 os.Exit；失败只记 Debug，避免经 Hook 回流到 Reporter。
func fatalExit(r *logreport.Reporter, exit func(int)) {
	if r != nil {
		if err := r.FlushFatal(fatalFlushTimeout); err != nil {
			log.WithField("err", err).Debug("[log] 退出前上报节点日志未完成")
		}
	}
	exit(1)
}

// replaySpool 带超时补发 spool 中上次未送达的报告，失败只记 Debug（剩余部分保留在文件中）。
func replaySpool(ctx context.Context, r *logreport.Reporter) {
	if r == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, replayTimeout)
	defer cancel()
	if err := r.ReplaySpool(ctx); err != nil {
		log.WithField("err", err).Debug("[log] 补发上次未送达的节点日志失败")
	}
}
