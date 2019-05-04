package common

import "errors"

var (
	ERROR_UNGET_LOCK   = errors.New("没有抢到锁")
	ERROR_IP_NOT_FOUND = errors.New("本机没有物理网卡或者ipv4地址")
)
