package vcontainer

import (
	"fmt"
	metadata2 "github.com/cuongpiger/vcontainer-ccm/pkg/vngcloud/utils/metadata"
	vconSdkClient "github.com/vngcloud/vcontainer-sdk/client"
	"github.com/vngcloud/vcontainer-sdk/vcontainer"
	lK8sCore "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	lcloudProvider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

type (
	VContainer struct {
		provider     *vconSdkClient.ProviderClient
		vLBOpts      VLBOpts
		metadataOpts metadata2.Opts
		config       *Config
		extraInfo    *ExtraInfo

		kubeClient       kubernetes.Interface
		eventBroadcaster record.EventBroadcaster
		eventRecorder    record.EventRecorder
	}

	ExtraInfo struct {
		ProjectID string
		UserID    int64
	}
)

func (s *VContainer) Initialize(clientBuilder lcloudProvider.ControllerClientBuilder, stop <-chan struct{}) {
	clientset := clientBuilder.ClientOrDie("cloud-controller-manager")
	s.kubeClient = clientset
	s.eventBroadcaster = record.NewBroadcaster()
	s.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: s.kubeClient.CoreV1().Events("")})
	s.eventRecorder = s.eventBroadcaster.NewRecorder(
		scheme.Scheme,
		lK8sCore.EventSource{Component: fmt.Sprintf("cloud-provider-%s", ProviderName)})
}

func (s *VContainer) LoadBalancer() (lcloudProvider.LoadBalancer, bool) {
	klog.V(4).Info("Set up LoadBalancer service for vcontainer-ccm")

	// Prepare the client for vLB
	vlb, _ := vcontainer.NewLoadBalancer(s.getVServerURL(), s.provider)
	return &vLB{
		vLBSC:         vlb,
		kubeClient:    s.kubeClient,
		eventRecorder: s.eventRecorder,
		extraInfo:     s.extraInfo,
	}, true
}

func (s *VContainer) Instances() (lcloudProvider.Instances, bool) {
	return nil, false
}

func (s *VContainer) InstancesV2() (lcloudProvider.InstancesV2, bool) {
	return nil, false
}

func (s *VContainer) Zones() (lcloudProvider.Zones, bool) {
	return nil, false
}

func (s *VContainer) Routes() (lcloudProvider.Routes, bool) {
	return nil, false
}

func (s *VContainer) Clusters() (lcloudProvider.Clusters, bool) {
	return nil, false
}

func (s *VContainer) ProviderName() string {
	return ProviderName
}

func (s *VContainer) HasClusterID() bool {
	return true
}

func (s *VContainer) getVServerURL() string {
	return s.config.Global.VServerURL
}
