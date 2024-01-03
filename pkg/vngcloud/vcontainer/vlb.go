package vcontainer

import (
	"context"
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/vngcloud/vcontainer-sdk/client"
)

type VLBOpts struct {
	Enabled        bool   `gcfg:"enabled"`         // if false, disables the controller
	InternalLB     bool   `gcfg:"internal-lb"`     // default false
	FlavorID       string `gcfg:"flavor-id"`       // flavor id of load balancer
	MaxSharedLB    int    `gcfg:"max-shared-lb"`   //  Number of Services in maximum can share a single load balancer. Default 2
	LBMethod       string `gcfg:"lb-method"`       // default to ROUND_ROBIN.
	EnableVMonitor bool   `gcfg:"enable-vmonitor"` // default to false
}

type vLB struct {
	vLBSC         *client.ServiceClient
	kubeClient    kubernetes.Interface
	eventRecorder record.EventRecorder
	extraInfo     *ExtraInfo
}

type serviceConfig struct {
	internal          bool
	lbID              string
	preferredIPFamily corev1.IPFamily // preferred (the first) IP family indicated in service's `spec.ipFamilies`
	flavorID          string
	scheme            loadbalancer.CreateOptsSchemeOpt
	lbType            loadbalancer.CreateOptsTypeOpt
}

func (s *vLB) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	return nil, false, nil
}

func (s *vLB) GetLoadBalancerName(_ context.Context, clusterName string, service *corev1.Service) string {
	return ""
}

func (s *vLB) EnsureLoadBalancer(ctx context.Context, clusterName string, apiService *corev1.Service, nodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {
	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", clusterName, "service", klog.KObj(apiService))
	status, err := s.ensureLoadBalancer(ctx, clusterName, apiService, nodes)
	return status, mc.ObserveReconcile(err)
}

func (s *vLB) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	return nil
}

func (s *vLB) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	return nil
}

// ************************************************** PRIVATE METHODS **************************************************

func (s *vLB) ensureLoadBalancer(pCtx context.Context, pClusterName string, pService *corev1.Service, pNodes []*corev1.Node) (rLbs *corev1.LoadBalancerStatus, rErr error) {
	//svcConf := new(serviceConfig)

	// Update the service annotations(e.g. add vcontainer.vngcloud.vn/load-balancer-id) in the end if it doesn't exist
	patcher := newServicePatcher(s.kubeClient, pService)
	defer func() {
		rErr = patcher.Patch(pCtx, rErr)
	}()

	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterName, "service", klog.KObj(pService))

	return nil, nil
}

func (s *vLB) checkService(pService *corev1.Service, pNodes []*corev1.Node, pServiceConfig *serviceConfig) error {
	serviceName := fmt.Sprintf("%s/%s", pService.Namespace, pService.Name)

	if len(pNodes) <= 0 {
		return fmt.Errorf("there are no available nodes for LoadBalancer service %s", serviceName)
	}

	ports := pService.Spec.Ports
	if len(ports) <= 0 {
		return fmt.Errorf("no service ports provided")
	}

	if len(pService.Spec.IPFamilies) > 0 {
		// Since the plugin does not support multiple load-balancers per service yet, the first IP family will determine the IP family of the load-balancer
		pServiceConfig.preferredIPFamily = pService.Spec.IPFamilies[0]
	}

	pServiceConfig.lbID = getStringFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerID, "")
	if pServiceConfig.preferredIPFamily == corev1.IPv6Protocol {
		pServiceConfig.internal = true
	} else {
		pServiceConfig.internal = getBoolFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerInternal, false)
	}

	return nil
}

func (s *vLB) createLoadBalancer(pLbName, pClusterName string, pService *corev1.Service, pNodes []*corev1.Node, pServiceConfig *serviceConfig) error {
	mc := metrics.NewMetricContext("loadbalancer", "create")
	klog.InfoS("CreateLoadBalancer", "cluster", pClusterName, "service", klog.KObj(pService), "lbName", pLbName)
	newLb, err := loadbalancer.Create(s.vLBSC, loadbalancer.NewCreateOpts(
		s.extraInfo.ProjectID,
		&loadbalancer.CreateOpts{
			Name:      pLbName,
			PackageID: pServiceConfig.flavorID,
			Scheme:    pServiceConfig.scheme,
			Type:      pServiceConfig.lbType,
		}))

	if mc.ObserveReconcile(err) != nil {
		return err
	}

	klog.Infof("Created load balancer %s for service %s", newLb.UUID, pService.Name)

	return nil
}
