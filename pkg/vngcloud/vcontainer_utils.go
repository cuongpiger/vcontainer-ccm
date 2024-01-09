package vngcloud

import (
	"fmt"
	"github.com/cuongpiger/joat/utils"
	"github.com/cuongpiger/vcontainer-ccm/pkg/client"
	lvconCcmMetrics "github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/metadata"
	client2 "github.com/vngcloud/vcontainer-sdk/client"
	lvconSdkErr "github.com/vngcloud/vcontainer-sdk/error/utils"
	"github.com/vngcloud/vcontainer-sdk/vcontainer"
	lPortal "github.com/vngcloud/vcontainer-sdk/vcontainer/services/portal/v1"
	gcfg "gopkg.in/gcfg.v1"
	"io"
	lcloudProvider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// ************************************************* PUBLIC FUNCTIONS **************************************************

func NewVContainer(pCfg Config) (*VContainer, error) {
	provider, err := client.NewVContainerClient(&pCfg.Global)
	if err != nil {
		klog.Errorf("failed to init VContainer client: %v", err)
		return nil, err
	}

	metadator := metadata.GetMetadataProvider(pCfg.Metadata.SearchOrder)
	extraInfo, err := setupPortalInfo(
		provider,
		metadator,
		utils.NormalizeURL(pCfg.Global.VServerURL)+"vserver-gateway/v1")

	if err != nil {
		klog.Errorf("failed to setup portal info: %v", err)
		return nil, err
	}

	return &VContainer{
		provider:  provider,
		vLBOpts:   pCfg.VLB,
		config:    &pCfg,
		extraInfo: extraInfo,
	}, nil
}

// ************************************************* PRIVATE FUNCTIONS *************************************************

func init() {
	fmt.Println("CUONGDM3: init the vcontainer-ccm")
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

			config.Metadata = getMetadataOption(config.Metadata)
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

func getMetadataOption(pMetadata metadata.Opts) metadata.Opts {
	if pMetadata.SearchOrder == "" {
		pMetadata.SearchOrder = fmt.Sprintf("%s,%s", metadata.ConfigDriveID, metadata.MetadataID)
	}
	klog.Info("getMetadataOption; metadataOpts is ", pMetadata)
	return pMetadata
}

func setupPortalInfo(pProvider *client2.ProviderClient, pMedatata metadata.IMetadata, pPortalURL string) (*ExtraInfo, error) {
	projectID, err := pMedatata.GetProjectID()
	if err != nil {
		return nil, err
	}
	klog.Infof("SetupPortalInfo; projectID is %s", projectID)

	portalClient, _ := vcontainer.NewPortal(pPortalURL, pProvider)
	portalInfo, err := lPortal.Get(portalClient, projectID)

	if err != nil {
		return nil, err
	}

	if portalInfo == nil {
		return nil, fmt.Errorf("can not get portal information")
	}

	return &ExtraInfo{
		ProjectID: portalInfo.ProjectID,
		UserID:    portalInfo.UserID,
	}, nil
}
