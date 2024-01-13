package vngcloud

import (
	"time"
)

const (
	waitLoadbalancerInitDelay   = 5 * time.Second
	waitLoadbalancerFactor      = 1.2
	waitLoadbalancerActiveSteps = 30
	waitLoadbalancerDeleteSteps = 12
)

const (
	PROVIDER_NAME              = "vngcloud"
	ACTIVE_LOADBALANCER_STATUS = "ACTIVE"
)

const (
	healthMonitorHealthyThreshold   = 3
	healthMonitorUnhealthyThreshold = 3
	healthMonitorTimeout            = 5
	healthMonitorInterval           = 30
)
