package vngcloud

import (
	"context"
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	obj2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener/obj"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer/obj"
	obj3 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool/obj"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"strings"

	"github.com/vngcloud/vcontainer-sdk/client"
)

const (
	lbFormat           = "%s%s_%s_%s"
	servicePrefix      = ""
	defaultL4PackageID = "lbp-96b6b072-aadb-4b58-9d5f-c16ad69d36aa"
	defaultL7PackageID = "lbp-f562b658-0fd4-4fa6-9c57-c1a803ccbf86"
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
	vServerSC     *client.ServiceClient
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
	clusterID         string
	projectID         string
	subnetID          string
}

type listenerKey struct {
	Protocol string
	Port     int
}

func (s *vLB) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	return nil, false, nil
}

func (s *vLB) GetLoadBalancerName(_ context.Context, clusterName string, service *corev1.Service) string {
	return utils.Sprintf50(lbFormat, servicePrefix, clusterName[:21], service.Namespace, service.Name)
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
	svcConf := &serviceConfig{
		clusterID: pClusterName,
		projectID: s.extraInfo.ProjectID,
	}

	createdNewLB := true // change to false later

	// Update the service annotations(e.g. add vcontainer.vngcloud.vn/load-balancer-id) in the end if it doesn't exist
	patcher := newServicePatcher(s.kubeClient, pService)
	defer func() {
		rErr = patcher.Patch(pCtx, rErr)
	}()

	if rErr = s.checkService(pService, pNodes, svcConf); rErr != nil {
		return nil, rErr
	}

	pLbName := s.GetLoadBalancerName(pCtx, pClusterName, pService)

	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterName, "service", klog.KObj(pService))
	lb, rErr := s.createLoadBalancer(pLbName, pClusterName, pService, pNodes, svcConf)

	if createdNewLB {
		curListeners, err := listener.GetBasedLoadBalancer(s.vLBSC, listener.NewGetBasedLoadBalancerOpts(s.extraInfo.ProjectID, lb.UUID))
		if err != nil {
			klog.Errorf("failed to get listeners for load balancer %s: %v", lb.UUID, err)
			return nil, err
		}

		if len(curListeners) > 0 {
			klog.Infof("Load balancer %s has %d listeners", lb.UUID, len(curListeners))
			curListenerMapping := make(map[listenerKey]*obj2.Listener)
			for i, l := range curListeners {
				key := listenerKey{
					Protocol: l.Protocol,
					Port:     l.ProtocolPort}
				curListenerMapping[key] = curListeners[i]
			}
			klog.V(4).InfoS("Existing listeners", "portProtocolMapping", curListenerMapping)

			if err = s.checkListenerPorts(pService, curListenerMapping, true, pLbName); err != nil {
				klog.Errorf("Detected conflict ports for load balancer %s: %v", lb.UUID, err)
				return nil, err
			}

			for portIndex, port := range pService.Spec.Ports {

			}
		}
	}

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

	itsCluster, err := cluster.Get(s.vServerSC, cluster.NewGetOpts(pServiceConfig.projectID, pServiceConfig.clusterID))
	if err != nil {
		klog.Errorf("failed to get cluster %s: %v", pServiceConfig.clusterID, err)
		return err
	}

	if itsCluster == nil {
		return fmt.Errorf("cluster %s not found", pServiceConfig.clusterID)
	}

	pServiceConfig.subnetID = itsCluster.SubnetID
	if !getBoolFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerInternal, false) {
		pServiceConfig.internal = true
	}

	switch lbType := getStringFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerType, "layer-4"); lbType {
	case "layer-7":
		pServiceConfig.lbType = loadbalancer.CreateOptsTypeOptLayer7
		pServiceConfig.flavorID = getStringFromServiceAnnotation(pService, ServiceAnnotationPackageID, defaultL7PackageID)
	default:
		pServiceConfig.lbType = loadbalancer.CreateOptsTypeOptLayer4
		pServiceConfig.flavorID = getStringFromServiceAnnotation(pService, ServiceAnnotationPackageID, defaultL4PackageID)
	}

	return nil
}

func (s *vLB) createLoadBalancer(pLbName, pClusterName string, pService *corev1.Service, pNodes []*corev1.Node, pServiceConfig *serviceConfig) (*obj.LoadBalancer, error) {
	opts := &loadbalancer.CreateOpts{
		Scheme:    loadbalancer.CreateOptsSchemeOptInternet,
		Type:      pServiceConfig.lbType,
		Name:      pLbName,
		PackageID: pServiceConfig.flavorID,
		SubnetID:  pServiceConfig.subnetID,
	}

	if pServiceConfig.internal {
		opts.Scheme = loadbalancer.CreateOptsSchemeOptInternal
	}

	mc := metrics.NewMetricContext("loadbalancer", "create")
	klog.InfoS("CreateLoadBalancer", "cluster", pClusterName, "service", klog.KObj(pService), "lbName", pLbName)
	newLb, err := loadbalancer.Create(s.vLBSC, loadbalancer.NewCreateOpts(s.extraInfo.ProjectID, opts))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create load balancer %s for service %s: %v", pLbName, pService.Name, err)
		return nil, err
	}

	klog.Infof("Created load balancer %s for service %s, waiting it will be ready", newLb.UUID, pService.Name)
	newLb, err = s.waitLoadBalancerReady(newLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", newLb.UUID, pService.Name, err)
		// delete this loadbalancer
		if err2 := loadbalancer.Delete(s.vLBSC, loadbalancer.NewDeleteOpts(s.extraInfo.ProjectID, newLb.UUID)); err2 != nil {
			klog.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
			return nil, fmt.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
		}

		return nil, err
	}

	return newLb, nil
}

func (s *vLB) waitLoadBalancerReady(pLbID string) (*obj.LoadBalancer, error) {
	klog.Infof("Waiting for load balancer %s to be ready", pLbID)
	var lb *obj.LoadBalancer

	err := wait.ExponentialBackoff(wait.Backoff{
		Duration: waitLoadbalancerInitDelay,
		Factor:   waitLoadbalancerFactor,
		Steps:    waitLoadbalancerActiveSteps,
	}, func() (done bool, err error) {
		mc := metrics.NewMetricContext("loadbalancer", "get")
		lb, err := loadbalancer.Get(s.vLBSC, loadbalancer.NewGetOpts(s.extraInfo.ProjectID, pLbID))
		if mc.ObserveReconcile(err) != nil {
			klog.Errorf("failed to get load balancer %s: %v", pLbID, err)
			return false, err
		}

		if strings.ToUpper(lb.Status) == ACTIVE_LOADBALANCER_STATUS {
			klog.Infof("Load balancer %s is ready", pLbID)
			return true, nil
		}

		klog.Infof("Load balancer %s is not ready yet, wating...", pLbID)
		return false, nil
	})

	if wait.Interrupted(err) {
		err = fmt.Errorf("timeout waiting for the loadbalancer %s with lb status %s", pLbID, lb.Status)
	}

	return lb, err
}

// checkListenerPorts checks if there is conflict for ports.
func (s *vLB) checkListenerPorts(service *corev1.Service, curListenerMapping map[listenerKey]*obj2.Listener, isLBOwner bool, lbName string) error {
	for _, svcPort := range service.Spec.Ports {
		key := listenerKey{Protocol: string(svcPort.Protocol), Port: int(svcPort.Port)}

		if l, isPresent := curListenerMapping[key]; isPresent {
			// The listener is used by this Service if LB name is in the tags, or
			// the listener was created by this Service.
			if isLBOwner || strings.Contains(lbName, l.Name[:-len(fmt.Sprintf("%d", l.ProtocolPort))]) {
				continue
			} else {
				return fmt.Errorf("the listener port %d already exists", svcPort.Port)
			}
		}
	}

	return nil
}

func (s *vLB) ensurePool(pLbID string, pService *corev1.Service, pNodes []*corev1.Node, pServiceConfig *serviceConfig) (*obj3.Pool, error) {
	return nil, nil
}
