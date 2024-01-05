package vngcloud

import (
	vconSdkClient "github.com/cuongpiger/vcontainer-ccm/pkg/client"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/metadata"
	"time"
)

type (
	Config struct {
		Global   vconSdkClient.AuthOpts
		VLB      VLBOpts
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
	ACTIVE_LOADBALANCER_STATUS = "ACTIVE"
)
