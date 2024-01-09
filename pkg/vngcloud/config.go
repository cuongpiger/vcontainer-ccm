package vngcloud

import (
	vconSdkClient "github.com/cuongpiger/vcontainer-ccm/pkg/client"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/metadata"
	"time"
)

type (
	Config struct {
		Global   vconSdkClient.AuthOpts
		VLB      VLbOpts
		Metadata metadata.Opts
	}
)

const (
	waitLoadbalancerInitDelay   = 5 * time.Second
	waitLoadbalancerFactor      = 1.2
	waitLoadbalancerActiveSteps = 30
	waitLoadbalancerDeleteSteps = 12
)

const (
	DEFAULT_PORTAL_NAME_LENGTH = 50
	ACTIVE_LOADBALANCER_STATUS = "ACTIVE"
)

const (
	healthMonitorHealthyThreshold   = 3
	healthMonitorUnhealthyThreshold = 3
	healthMonitorTimeout            = 5
	healthMonitorInterval           = 30
)

const (
	listenerDefaultCIDR       = "0.0.0.0/0"
	listenerTimeoutClient     = 50
	listenerTimeoutConnection = 5
	listenerTimeoutMember     = 50
)
