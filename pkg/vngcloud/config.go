package vngcloud

import (
	vconSdkClient "github.com/cuongpiger/vcontainer-ccm/pkg/client"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/metadata"
)

type (
	Config struct {
		Global   vconSdkClient.AuthOpts
		VLB      VLBOpts
		Metadata metadata.Opts
	}
)
