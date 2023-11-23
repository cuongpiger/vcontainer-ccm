package vcontainer

import (
	vconSdkClient "github.com/cuongpiger/vcontainer-ccm/pkg/client"
	"github.com/cuongpiger/vcontainer-ccm/pkg/vngcloud/utils/metadata"
)

type (
	Config struct {
		Global   vconSdkClient.AuthOpts
		VLB      VLBOpts
		Metadata metadata.Opts
	}
)
