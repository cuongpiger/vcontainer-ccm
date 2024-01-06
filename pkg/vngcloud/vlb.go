package vngcloud

import (
	"context"
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/errors"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster"
	lListenerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	lListenerObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener/obj"
	lLoadBalancerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	lLbObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer/obj"
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

type listenerKey struct {
	Protocol string
	Port     int
}

func (s *vLB) GetLoadBalancer(pCtx context.Context, pClusterID string, pService *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("GetLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, existed, err := s.ensureGetLoadBalancer(pCtx, pClusterID, pService)
	return status, existed, mc.ObserveReconcile(err)
}

func (s *vLB) GetLoadBalancerName(_ context.Context, pClusterName string, pService *corev1.Service) string {
	return utils.GenLoadBalancerName(pClusterName, pService)
}

func (s *vLB) EnsureLoadBalancer(
	pCtx context.Context, pClusterID string, pService *corev1.Service, pNodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {

	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, err := s.ensureLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return status, mc.ObserveReconcile(err)
}

func (s *vLB) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	return nil
}

func (s *vLB) EnsureLoadBalancerDeleted(pCtx context.Context, pClusterID string, pService *corev1.Service) error {
	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancerDeleted", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.ensureDeleteLoadBalancer(pCtx, pClusterID, pService)
	return mc.ObserveReconcile(err)
}

// ************************************************** PRIVATE METHODS **************************************************

func (s *vLB) ensureLoadBalancer(
	pCtx context.Context, pClusterID string, pService *corev1.Service, pNodes []*corev1.Node) (rLbs *corev1.LoadBalancerStatus, rErr error) {

	patcher := newServicePatcher(s.kubeClient, pService)
	defer func() {
		rErr = patcher.Patch(pCtx, rErr)
	}()

	// Unknow the variable
	var userLb *lLbObjV2.LoadBalancer

	// Check the loadbalancer of this service is existed or not
	createdNewLB := false
	isOwner := false

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := cluster.Get(s.vServerSC, cluster.NewGetOpts(s.getProjectID(), pClusterID))
	if cluster.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.ListBySubnetID(s.vLBSC, lLoadBalancerV2.NewListBySubnetIDOpts(s.getProjectID(), userCluster.SubnetID))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return nil, err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	if len(userLbs) < 1 {
		klog.Infof("there is no load balancer for cluster %s in the subnet %s, creating new one", pClusterID, userCluster.SubnetID)
		createdNewLB = true
		isOwner = true
	} else if userLb = s.findLoadBalancer(lbName, userLbs); userLb == nil {
		createdNewLB = true
		isOwner = true
	} else if userLb != nil {
		isOwner = true
	}

	svcConf := &serviceConfig{
		cluster:   userCluster,
		projectID: s.getProjectID(),
		isOwner:   isOwner,
	}

	if err = s.checkService(pService, pNodes, svcConf); err != nil {
		return nil, err
	}

	// Create the loadbalancer if it's not existed
	if createdNewLB {
		klog.Infof("Creating load balancer %s for service %s/%s", lbName, pService.Namespace, pService.Name)
		userLb, err = s.createLoadBalancer(lbName, pClusterID, pService, svcConf)

		if err != nil {
			klog.Errorf(
				"failed to create load balancer %s for service %s/%s: %v", lbName, pService.Namespace, pService.Name, err)
			return nil, err
		}
	}

	lbListeners, err := lListenerV2.GetBasedLoadBalancer(
		s.vLBSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID))
	if err != nil {
		klog.Errorf("failed to get listeners for load balancer %s: %v", userLb.UUID, err)
		return nil, err
	}

	curListenerMapping := make(map[listenerKey]*lListenerObjV2.Listener)
	for i, itemListener := range lbListeners {
		key := listenerKey{Protocol: itemListener.Protocol, Port: itemListener.ProtocolPort}
		curListenerMapping[key] = lbListeners[i]
	}

	for _, itemPort := range pService.Spec.Ports {
		newPool, err := s.ensurePool(userLb.UUID, pService, itemPort, pNodes, svcConf, createdNewLB)
		if err != nil {
			return nil, err
		}

		if !createdNewLB {
			err = s.checkListenerPorts(pService, curListenerMapping, lbName)
			if err != nil {
				klog.Errorf("the port and protocol is conflict: %v", err)
				return nil, err
			}
		}
		newListener, err := s.ensureListener(userLb.UUID, newPool.UUID, lbName, itemPort)
		if err != nil {
			klog.Errorf("failed to create listener for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		popListener(lbListeners, newListener.ID)
	}

	for _, itemListener := range lbListeners {
		err = lListenerV2.Delete(s.vLBSC, lListenerV2.NewDeleteOpts(s.getProjectID(), userLb.UUID, itemListener.ID))
		if err != nil {
			klog.Errorf("failed to delete listener %s for load balancer %s: %v", itemListener.ID, userLb.UUID, err)
			return nil, err
		}

		_, err = s.waitLoadBalancerReady(userLb.UUID)
		if err != nil {
			klog.Errorf("failed to wait load balancer %s for service %s: %v", userLb.UUID, pService.Name, err)
			return nil, err
		}
	}

	lbStatus := s.createLoadBalancerStatus(userLb.Address)

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

	// If the service is
	if pServiceConfig.preferredIPFamily == corev1.IPv6Protocol {
		pServiceConfig.internal = true
	} else {
		pServiceConfig.internal = getBoolFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerInternal, false)
	}

	pServiceConfig.subnetID = pServiceConfig.getClusterSubnetID()
	if !getBoolFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerInternal, false) {
		pServiceConfig.internal = true
	}

	switch lbType := getStringFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerType, "layer-4"); lbType {
	case "layer-7":
		pServiceConfig.lbType = lLoadBalancerV2.CreateOptsTypeOptLayer7
		pServiceConfig.flavorID = getStringFromServiceAnnotation(pService, ServiceAnnotationPackageID, defaultL7PackageID)
	default:
		pServiceConfig.lbType = lLoadBalancerV2.CreateOptsTypeOptLayer4
		pServiceConfig.flavorID = getStringFromServiceAnnotation(pService, ServiceAnnotationPackageID, defaultL4PackageID)
	}

	return nil
}

func (s *vLB) createLoadBalancer(pLbName, pClusterName string, pService *corev1.Service, pServiceConfig *serviceConfig) (*lLbObjV2.LoadBalancer, error) {
	opts := &lLoadBalancerV2.CreateOpts{
		Scheme:    lLoadBalancerV2.CreateOptsSchemeOptInternet,
		Type:      pServiceConfig.lbType,
		Name:      pLbName,
		PackageID: pServiceConfig.flavorID,
		SubnetID:  pServiceConfig.subnetID,
	}

	if pServiceConfig.internal {
		opts.Scheme = lLoadBalancerV2.CreateOptsSchemeOptInternal
	}

	mc := metrics.NewMetricContext("loadbalancer", "create")
	klog.InfoS("CreateLoadBalancer", "cluster", pClusterName, "service", klog.KObj(pService), "lbName", pLbName)
	newLb, err := lLoadBalancerV2.Create(s.vLBSC, lLoadBalancerV2.NewCreateOpts(s.extraInfo.ProjectID, opts))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create load balancer %s for service %s: %v", pLbName, pService.Name, err)
		return nil, err
	}

	klog.Infof("Created load balancer %s for service %s, waiting it will be ready", newLb.UUID, pService.Name)
	newLb, err = s.waitLoadBalancerReady(newLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", newLb.UUID, pService.Name, err)
		// delete this loadbalancer
		if err2 := lLoadBalancerV2.Delete(s.vLBSC, lLoadBalancerV2.NewDeleteOpts(s.extraInfo.ProjectID, newLb.UUID)); err2 != nil {
			klog.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
			return nil, fmt.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
		}

		return nil, err
	}

	return newLb, nil
}

func (s *vLB) waitLoadBalancerReady(pLbID string) (*lLbObjV2.LoadBalancer, error) {
	klog.Infof("Waiting for load balancer %s to be ready", pLbID)
	var lb *lLbObjV2.LoadBalancer

	err := wait.ExponentialBackoff(wait.Backoff{
		Duration: waitLoadbalancerInitDelay,
		Factor:   waitLoadbalancerFactor,
		Steps:    waitLoadbalancerActiveSteps,
	}, func() (done bool, err error) {
		mc := metrics.NewMetricContext("loadbalancer", "get")
		lb, err := lLoadBalancerV2.Get(s.vLBSC, lLoadBalancerV2.NewGetOpts(s.extraInfo.ProjectID, pLbID))
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
func (s *vLB) checkListenerPorts(service *corev1.Service, curListenerMapping map[listenerKey]*lListenerObjV2.Listener, pClusterID string) error {
	for _, svcPort := range service.Spec.Ports {
		key := listenerKey{Protocol: string(svcPort.Protocol), Port: int(svcPort.Port)}

		if lbListener, isPresent := curListenerMapping[key]; isPresent {
			// The listener is used by this Service if LB name is in the tags, or
			// the listener was created by this Service.
			if lbListener.Name == utils.GenListenerName(pClusterID, service, string(svcPort.Protocol), int(svcPort.Port)) {
				continue
			} else {
				return fmt.Errorf("the listener port %d already exists, can not use", svcPort.Port)
			}
		}
	}

	return nil
}

func (s *vLB) ensurePool(
	pLbID string, pService *corev1.Service, pPort corev1.ServicePort, pNodes []*corev1.Node, pServiceConfig *serviceConfig, createdNewLb bool) (*obj3.Pool, error) {

	// Get the pool name
	poolName := utils.GenPoolName(pLbID, pService, string(pPort.Protocol))

	// Check if the pool is existed
	if !createdNewLb {
		// Get all pools of this load-balancer depends on the load-balancer UUID
		pools, err := pool.ListPoolsBasedLoadBalancer(s.vLBSC, pool.NewListPoolsBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLbID))
		if err != nil {
			klog.Errorf("failed to list pools for load balancer %s: %v", pLbID, err)
			return nil, err
		}

		for _, itemPool := range pools {
			if itemPool.Name == poolName {
				err = pool.Delete(s.vLBSC, pool.NewDeleteOpts(s.extraInfo.ProjectID, pLbID, itemPool.UUID))
				if err != nil {
					klog.Errorf("failed to delete pool %s for service %s: %v", itemPool.Name, pService.Name, err)
					return nil, err
				}

				break
			}
		}

		_, err = s.waitLoadBalancerReady(pLbID)
		if err != nil {
			klog.Errorf("failed to wait load balancer %s for service %s: %v", pLbID, pService.Name, err)
			return nil, err
		}
	}

	poolMembers, err := prepareMembers4Pool(pNodes, pPort, pServiceConfig)
	if err != nil {
		klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
		return nil, err
	}

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

	if err != nil {
		klog.Errorf("failed to create pool %s for service %s: %v", pService.Name, pService.Name, err)
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

func (s *vLB) ensureListener(pLbID, pPoolID, pLbName string, pPort corev1.ServicePort) (*lListenerObjV2.Listener, error) {
	mc := metrics.NewMetricContext("listener", "create")
	newListener, err := lListenerV2.Create(s.vLBSC, lListenerV2.NewCreateOpts(
		s.extraInfo.ProjectID,
		pLbID,
		&lListenerV2.CreateOpts{
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

func (s *vLB) getProjectID() string {
	return s.extraInfo.ProjectID
}

func (s *vLB) findLoadBalancer(pLbName string, pLbs []*lLbObjV2.LoadBalancer) *lLbObjV2.LoadBalancer {
	for _, lb := range pLbs {
		if lb.Name == pLbName {
			return lb
		}
	}

	return nil
}

func (s *vLB) ensureDeleteLoadBalancer(pCtx context.Context, pClusterID string, pService *corev1.Service) error {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := cluster.Get(s.vServerSC, cluster.NewGetOpts(s.getProjectID(), pClusterID))
	if cluster.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.ListBySubnetID(s.vLBSC, lLoadBalancerV2.NewListBySubnetIDOpts(s.getProjectID(), userCluster.SubnetID))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	for _, itemLb := range userLbs {
		if itemLb.Name == lbName {
			err := lLoadBalancerV2.Delete(s.vLBSC, lLoadBalancerV2.NewDeleteOpts(s.getProjectID(), itemLb.UUID))
			if err != nil {
				klog.Errorf("failed to delete load balancer %s: %v", itemLb.UUID, err)
				return err
			}

			break
		}
	}

	return nil
}

func (s *vLB) ensureGetLoadBalancer(pCtx context.Context, pClusterID string, pService *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := cluster.Get(s.vServerSC, cluster.NewGetOpts(s.getProjectID(), pClusterID))
	if cluster.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, false, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.ListBySubnetID(s.vLBSC, lLoadBalancerV2.NewListBySubnetIDOpts(s.getProjectID(), userCluster.SubnetID))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return nil, false, err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	for _, itemLb := range userLbs {
		if itemLb.Name == lbName {
			status := &corev1.LoadBalancerStatus{}
			status.Ingress = []corev1.LoadBalancerIngress{{IP: itemLb.Address}}
			return status, true, nil
		}
	}

	return nil, false, fmt.Errorf("the loadbalancer %s is not existed", lbName)
}

func popListener(pExistingListeners []*lListenerObjV2.Listener, pNewListenerID string) []*lListenerObjV2.Listener {
	var newListeners []*lListenerObjV2.Listener

	for _, existingListener := range pExistingListeners {
		if existingListener.ID != pNewListenerID {
			newListeners = append(newListeners, existingListener)
		}
	}
	return newListeners
}
