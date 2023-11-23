package vcontainer

import (
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/client"
	lvconCcmMetrics "github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/cuongpiger/vcontainer-ccm/pkg/vngcloud/utils/metadata"
	lvconSdkErr "github.com/vngcloud/vcontainer-sdk/error/utils"
	gcfg "gopkg.in/gcfg.v1"
	"io"
	lcloudProvider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// ************************************************* PUBLIC FUNCTIONS **************************************************

func NewVContainer(pCfg Config) (*VContainer, error) {
	provider, err := client.NewVContainerClient(&pCfg.Global)
	vcontainer := &VContainer{
		provider: provider,
		vLBOpts:  pCfg.VLB,
	}

	return vcontainer, err
}

// ************************************************* PRIVATE FUNCTIONS *************************************************

func init() {
	// Register metrics
	lvconCcmMetrics.RegisterMetrics("vcontainer-ccm")

	// Register VNG-CLOUD cloud provider
	lcloudProvider.RegisterCloudProvider(
		ProviderName,
		func(cfg io.Reader) (lcloudProvider.Interface, error) {
			config, err := readConfig(cfg)
			if err != nil {
				klog.Warningf("failed to read config file: %v", err)
				return nil, err
			}

			cloud, err := NewVContainer(config)
			if err != nil {
				klog.Warningf("failed to init VContainer: %v", err)
			}

			return cloud, err
		},
	)
}

func readConfig(pCfg io.Reader) (Config, error) {
	if pCfg == nil {
		return Config{}, lvconSdkErr.NewErrEmptyConfig("", "config file is empty")
	}

	// Set default
	config := Config{}
	config.VLB.Enabled = true
	// TODO: Read config file

	err := gcfg.FatalOnly(gcfg.ReadInto(&config, pCfg))
	if err != nil {
		return Config{}, err
	}

	// Log the config
	klog.V(5).Infof("read config from file")
	client.LogCfg(config.Global)

	if config.Metadata.SearchOrder == "" {
		config.Metadata.SearchOrder = fmt.Sprintf("%s,%s", metadata.ConfigDriveID, metadata.MetadataID)
	}

	return config, nil
}
