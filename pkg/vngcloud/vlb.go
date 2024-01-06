package vngcloud

import (
	"context"
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/errors"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	obj2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener/obj"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer/obj"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"
	obj3 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool/obj"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
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
		for _, port := range pService.Spec.Ports {
			pool, err := s.ensurePool(lb.UUID, pService, port, pNodes, svcConf)
			if err != nil {
				return nil, err
			}

			_, err = s.ensureListener(lb.UUID, pool.UUID, pLbName, port)
			if err != nil {
				return nil, err
			}
		}
	}

	lbStatus := s.createLoadBalancerStatus(lb.Address)

	return lbStatus, nil
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

func (s *vLB) ensurePool(pLbID string, pService *corev1.Service, pPort corev1.ServicePort, pNodes []*corev1.Node, pServiceConfig *serviceConfig) (*obj3.Pool, error) {
	// Get all pools of this load-balancer depends on the load-balancer UUID
	//pools, err := pool.ListPoolsBasedLoadBalancer(s.vLBSC, pool.NewListPoolsBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLbID))
	//if err != nil {
	//	return nil, err
	//}

	poolMembers, err := prepareMembers4Pool(pNodes, pPort, pServiceConfig)
	if err != nil {
		klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
		return nil, err
	}

	mc := metrics.NewMetricContext("pool", "create")
	newPool, err := pool.Create(s.vLBSC, pool.NewCreateOpts(s.extraInfo.ProjectID, pLbID, &pool.CreateOpts{
		Algorithm:    pool.CreateOptsAlgorithmOptRoundRobin,
		PoolName:     pService.Name,
		PoolProtocol: utils.GetVLBProtocolOpt(pPort),
		Members:      poolMembers,
		HealthMonitor: pool.HealthMonitor{
			HealthCheckProtocol: string(utils.GetVLBProtocolOpt(pPort)),
			HealthyThreshold:    healthMonitorHealthyThreshold,
			UnhealthyThreshold:  healthMonitorUnhealthyThreshold,
			Timeout:             healthMonitorTimeout,
			Interval:            healthMonitorInterval,
		},
	}))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create pool %s: %v", pService.Name, err)
		return nil, err
	}

	klog.Infof("Created pool %s for service %s, waiting the loadbalancer update completely", pService.Name, pService.Name)
	_, err = s.waitLoadBalancerReady(pLbID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", pLbID, pService.Name, err)
		return nil, err
	}

	return newPool, nil
}

func prepareMembers4Pool(pNodes []*corev1.Node, pPort corev1.ServicePort, pServiceConfig *serviceConfig) ([]pool.Member, error) {
	var poolMembers []pool.Member

	for _, itemNode := range pNodes {
		nodeAddress, err := nodeAddressForLB(itemNode, pServiceConfig.preferredIPFamily)
		if err != nil {
			if errors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

		// It's 0 when AllocateLoadBalancerNodePorts=False
		if pPort.NodePort != 0 {
			poolMembers = append(poolMembers, pool.Member{
				Backup:      true,
				IpAddress:   nodeAddress,
				Port:        int(pPort.NodePort),
				Weight:      1,
				MonitorPort: int(pPort.NodePort),
			})
		}
	}

	return poolMembers, nil
}

func nodeAddressForLB(node *corev1.Node, preferredIPFamily corev1.IPFamily) (string, error) {
	addrs := node.Status.Addresses
	if len(addrs) == 0 {
		return "", errors.NewErrNodeAddressNotFound(node.Name, "")
	}

	allowedAddrTypes := []corev1.NodeAddressType{corev1.NodeInternalIP, corev1.NodeExternalIP}

	for _, allowedAddrType := range allowedAddrTypes {
		for _, addr := range addrs {
			if addr.Type == allowedAddrType {
				switch preferredIPFamily {
				case corev1.IPv4Protocol:
					if netutils.IsIPv4String(addr.Address) {
						return addr.Address, nil
					}
				case corev1.IPv6Protocol:
					if netutils.IsIPv6String(addr.Address) {
						return addr.Address, nil
					}
				default:
					return addr.Address, nil
				}
			}
		}
	}

	return "", errors.NewErrNodeAddressNotFound(node.Name, "")
}

func (s *vLB) ensureListener(pLbID, pPoolID, pLbName string, pPort corev1.ServicePort) (*obj2.Listener, error) {
	mc := metrics.NewMetricContext("listener", "create")
	newListener, err := listener.Create(s.vLBSC, listener.NewCreateOpts(
		s.extraInfo.ProjectID,
		pLbID,
		&listener.CreateOpts{
			AllowedCidrs:         listenerDefaultCIDR,
			DefaultPoolId:        pPoolID,
			ListenerName:         pLbName,
			ListenerProtocol:     utils.GetListenerProtocolOpt(pPort),
			ListenerProtocolPort: int(pPort.Port),
			TimeoutClient:        listenerTimeoutClient,
			TimeoutMember:        listenerTimeoutMember,
			TimeoutConnection:    listenerTimeoutConnection,
		},
	))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create listener %s: %v", pLbName, err)
		return nil, err
	}

	return newListener, nil
}

func (s *vLB) createLoadBalancerStatus(addr string) *corev1.LoadBalancerStatus {
	status := &corev1.LoadBalancerStatus{}
	// Default to IP
	status.Ingress = []corev1.LoadBalancerIngress{{IP: addr}}
	return status
}
