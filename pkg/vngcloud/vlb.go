package vngcloud

import (
	"context"
	"fmt"
	"github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	lUtils "github.com/cuongpiger/vcontainer-ccm/pkg/utils"
	"github.com/cuongpiger/vcontainer-ccm/pkg/utils/errors"
	lObjects "github.com/vngcloud/vcontainer-sdk/vcontainer/objects"
	lClusterV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster"
	lClusterObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster/obj"
	lListenerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	lLoadBalancerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	lLbObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer/obj"
	lPoolV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"
	lSecRuleV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/network/v2/extensions/secgroup_rule"
	lSecgroupV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/network/v2/secgroup"
	lSubnetV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/network/v2/subnet"
	lCoreV1 "k8s.io/api/core/v1"
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

func (s *vLB) GetLoadBalancer(pCtx context.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("GetLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, existed, err := s.ensureGetLoadBalancer(pCtx, pClusterID, pService)
	return status, existed, mc.ObserveReconcile(err)
}

func (s *vLB) GetLoadBalancerName(_ context.Context, pClusterName string, pService *lCoreV1.Service) string {
	return lUtils.GenLoadBalancerName(pClusterName, pService)
}

func (s *vLB) EnsureLoadBalancer(
	pCtx context.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error) {

	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, err := s.ensureLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return status, mc.ObserveReconcile(err)
}

func (s *vLB) UpdateLoadBalancer(ctx context.Context, clusterName string, service *lCoreV1.Service, nodes []*lCoreV1.Node) error {
	return nil
}

func (s *vLB) EnsureLoadBalancerDeleted(pCtx context.Context, pClusterID string, pService *lCoreV1.Service) error {
	mc := metrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancerDeleted", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.ensureDeleteLoadBalancer(pCtx, pClusterID, pService)
	return mc.ObserveReconcile(err)
}

// ************************************************** PRIVATE METHODS **************************************************

func (s *vLB) ensureLoadBalancer(
	pCtx context.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (rLbs *lCoreV1.LoadBalancerStatus, rErr error) {

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
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLBSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
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
	} else if userLb = s.findLoadBalancer(lbName, userLbs, userCluster); userLb == nil {
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

	if err = s.checkService(pService, pNodes, svcConf, userLb); err != nil {
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

		klog.Infof("Load-balancer [%s] is ACTIVE with UUID [%s]", userLb.Name, userLb.UUID)
	}

	lbListeners, err := lListenerV2.GetBasedLoadBalancer(
		s.vLBSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID))
	if err != nil {
		klog.Errorf("failed to get listeners for load balancer %s: %v", userLb.UUID, err)
		return nil, err
	}
	klog.Infof("Load-balancer [%s] has [%d] listeners", userLb.Name, len(lbListeners))

	curListenerMapping := make(map[listenerKey]*lObjects.Listener)
	for i, itemListener := range lbListeners {
		key := listenerKey{Protocol: itemListener.Protocol, Port: itemListener.ProtocolPort}
		curListenerMapping[key] = lbListeners[i]
	}

	// This loadbalancer is existed, check the listener and pool, if existed, delete them
	if !createdNewLB {
		err = s.checkListenerPorts(pService, curListenerMapping, userLb.UUID)
		if err != nil {
			klog.Errorf("the port and protocol is conflict: %v", err)
			return nil, err
		}
	}

	for _, itemPort := range pService.Spec.Ports {
		klog.V(5).Infof("Processing pool using port %d", itemPort.Port)
		newPool, err := s.ensurePool(userCluster, userLb, pService, itemPort, pNodes, svcConf, createdNewLB)
		if err != nil {
			return nil, err
		}

		klog.V(5).Infof("Processing listener using port %d", itemPort.Port)
		newListener, err := s.ensureListener(userCluster, userLb.UUID, newPool.UUID, lbName, itemPort, pService)
		if err != nil {
			klog.Errorf("failed to create listener for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		// Update the security group
		if err = s.ensureSecgroup(userLb, userCluster, pService, itemPort, svcConf); err != nil {
			klog.Errorf("failed to update security group for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		popListener(lbListeners, newListener.ID)
	}
	klog.V(5).Infof("Processing listeners and pools completely, next to delete the unused listeners and pools")

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

	klog.Infof(
		"Load balancer %s for service %s/%s is ready to use for Kubernetes controller",
		lbName, pService.Namespace, pService.Name)
	return lbStatus, nil
}

func (s *vLB) checkService(pService *lCoreV1.Service, pNodes []*lCoreV1.Node, pServiceConfig *serviceConfig, pLb *lLbObjV2.LoadBalancer) error {
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
	if pServiceConfig.preferredIPFamily == lCoreV1.IPv6Protocol {
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

func (s *vLB) createLoadBalancer(pLbName, pClusterName string, pService *lCoreV1.Service, pServiceConfig *serviceConfig) (*lLbObjV2.LoadBalancer, error) {
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

	klog.Infof("Created load balancer %s for service %s successfully", newLb.UUID, pService.Name)
	return newLb, nil
}

func (s *vLB) waitLoadBalancerReady(pLbID string) (*lLbObjV2.LoadBalancer, error) {
	klog.Infof("Waiting for load balancer %s to be ready", pLbID)
	var resultLb *lLbObjV2.LoadBalancer

	err := wait.ExponentialBackoff(wait.Backoff{
		Duration: waitLoadbalancerInitDelay,
		Factor:   waitLoadbalancerFactor,
		Steps:    waitLoadbalancerActiveSteps,
	}, func() (done bool, err error) {
		mc := metrics.NewMetricContext("loadbalancer", "get")
		lb, err := lLoadBalancerV2.Get(s.vLBSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), pLbID))
		if mc.ObserveReconcile(err) != nil {
			klog.Errorf("failed to get load balancer %s: %v", pLbID, err)
			return false, err
		}

		if strings.ToUpper(lb.Status) == ACTIVE_LOADBALANCER_STATUS {
			klog.Infof("Load balancer %s is ready", pLbID)
			resultLb = lb
			return true, nil
		}

		klog.Infof("Load balancer %s is not ready yet, wating...", pLbID)
		return false, nil
	})

	if wait.Interrupted(err) {
		err = fmt.Errorf("timeout waiting for the loadbalancer %s with lb status %s", pLbID, resultLb.Status)
	}

	return resultLb, err
}

// checkListenerPorts checks if there is conflict for ports.
func (s *vLB) checkListenerPorts(service *lCoreV1.Service, curListenerMapping map[listenerKey]*lObjects.Listener, pLbID string) error {
	for _, svcPort := range service.Spec.Ports {
		key := listenerKey{Protocol: string(svcPort.Protocol), Port: int(svcPort.Port)}

		if lbListener, isPresent := curListenerMapping[key]; isPresent {
			// Delete this listener and its pools
			defaultPoolID := lbListener.DefaultPoolUUID
			if err := lListenerV2.Delete(s.vLBSC, lListenerV2.NewDeleteOpts(s.getProjectID(), pLbID, lbListener.ID)); err != nil {
				klog.Errorf("failed to delete listener %s for load balancer %s: %v", lbListener.ID, pLbID, err)
				return err
			}

			if _, err := s.waitLoadBalancerReady(pLbID); err != nil {
				klog.Errorf("failed to wait load balancer %s for service %s: %v", pLbID, service.Name, err)
				return err
			}

			if err := lPoolV2.Delete(s.vLBSC, lPoolV2.NewDeleteOpts(s.getProjectID(), pLbID, defaultPoolID)); err != nil {
				klog.Errorf("failed to delete pool %s for load balancer %s: %v", defaultPoolID, pLbID, err)
				return err
			}

			if _, err := s.waitLoadBalancerReady(pLbID); err != nil {
				klog.Errorf("failed to wait load balancer %s for service %s: %v", pLbID, service.Name, err)
				return err
			}
		}
	}

	return nil
}

func (s *vLB) ensurePool(
	pCluster *lClusterObjV2.Cluster, pLb *lLbObjV2.LoadBalancer, pService *lCoreV1.Service, pPort lCoreV1.ServicePort,
	pNodes []*lCoreV1.Node, pServiceConfig *serviceConfig, createdNewLb bool) (*lObjects.Pool, error) {

	// Get the pool name
	poolName := lUtils.GenPoolName(pCluster.ID, pService, string(pPort.Protocol))

	// Check if the pool is existed
	if !createdNewLb {
		// Get all pools of this load-balancer depends on the load-balancer UUID
		pools, err := lPoolV2.ListPoolsBasedLoadBalancer(
			s.vLBSC,
			lPoolV2.NewListPoolsBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLb.UUID))
		if err != nil {
			klog.Errorf("failed to list pools for load balancer %s: %v", pLb.UUID, err)
			return nil, err
		}

		for _, itemPool := range pools {
			if itemPool.Name == poolName {
				err = lPoolV2.Delete(s.vLBSC, lPoolV2.NewDeleteOpts(s.extraInfo.ProjectID, pLb.UUID, itemPool.UUID))
				if err != nil {
					klog.Errorf("failed to delete pool %s for service %s: %v", itemPool.Name, pService.Name, err)
					return nil, err
				}

				break
			}
		}

		_, err = s.waitLoadBalancerReady(pLb.UUID)
		if err != nil {
			klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
			return nil, err
		}
	}

	poolMembers, err := prepareMembers4Pool(pNodes, pPort, pServiceConfig)
	if err != nil {
		klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
		return nil, err
	}

	newPool, err := lPoolV2.Create(s.vLBSC, lPoolV2.NewCreateOpts(s.extraInfo.ProjectID, pLb.UUID, &lPoolV2.CreateOpts{
		Algorithm:    lPoolV2.CreateOptsAlgorithmOptRoundRobin,
		PoolName:     poolName,
		PoolProtocol: lUtils.GetVLBProtocolOpt(pPort),
		Members:      poolMembers,
		HealthMonitor: lPoolV2.HealthMonitor{
			HealthCheckProtocol: string(lUtils.GetVLBProtocolOpt(pPort)),
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
	_, err = s.waitLoadBalancerReady(pLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
		return nil, err
	}

	return newPool, nil
}

func prepareMembers4Pool(pNodes []*lCoreV1.Node, pPort lCoreV1.ServicePort, pServiceConfig *serviceConfig) ([]lPoolV2.Member, error) {
	var poolMembers []lPoolV2.Member

	for _, itemNode := range pNodes {
		// Ignore master node
		if _, ok := itemNode.GetObjectMeta().GetLabels()["node-role.kubernetes.io/master"]; ok {
			continue
		}

		_, err := getNodeAddressForLB(itemNode)
		if err != nil {
			if errors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

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
			poolMembers = append(poolMembers, lPoolV2.Member{
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

func nodeAddressForLB(node *lCoreV1.Node, preferredIPFamily lCoreV1.IPFamily) (string, error) {
	addrs := node.Status.Addresses
	if len(addrs) == 0 {
		return "", errors.NewErrNodeAddressNotFound(node.Name, "")
	}

	allowedAddrTypes := []lCoreV1.NodeAddressType{lCoreV1.NodeInternalIP, lCoreV1.NodeExternalIP}

	for _, allowedAddrType := range allowedAddrTypes {
		for _, addr := range addrs {
			if addr.Type == allowedAddrType {
				switch preferredIPFamily {
				case lCoreV1.IPv4Protocol:
					if netutils.IsIPv4String(addr.Address) {
						return addr.Address, nil
					}
				case lCoreV1.IPv6Protocol:
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

func (s *vLB) ensureListener(pCluster *lClusterObjV2.Cluster, pLbID, pPoolID, pLbName string, pPort lCoreV1.ServicePort, pService *lCoreV1.Service) (*lObjects.Listener, error) {

	mc := metrics.NewMetricContext("listener", "create")
	newListener, err := lListenerV2.Create(s.vLBSC, lListenerV2.NewCreateOpts(
		s.extraInfo.ProjectID,
		pLbID,
		&lListenerV2.CreateOpts{
			AllowedCidrs:         listenerDefaultCIDR,
			DefaultPoolId:        pPoolID,
			ListenerName:         lUtils.GenListenerName(pCluster.ID, pService, string(pPort.Protocol), int(pPort.Port)),
			ListenerProtocol:     lUtils.GetListenerProtocolOpt(pPort),
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

func (s *vLB) createLoadBalancerStatus(addr string) *lCoreV1.LoadBalancerStatus {
	status := &lCoreV1.LoadBalancerStatus{}
	// Default to IP
	status.Ingress = []lCoreV1.LoadBalancerIngress{{IP: addr}}
	return status
}

func (s *vLB) getProjectID() string {
	return s.extraInfo.ProjectID
}

func (s *vLB) findLoadBalancer(pLbName string, pLbs []*lLbObjV2.LoadBalancer, pCluster *lClusterObjV2.Cluster) *lLbObjV2.LoadBalancer {
	for _, lb := range pLbs {
		if lb.SubnetID == pCluster.SubnetID && lb.Name == pLbName {
			return lb
		}
	}

	return nil
}

func (s *vLB) ensureDeleteLoadBalancer(pCtx context.Context, pClusterID string, pService *lCoreV1.Service) error {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLBSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	lbUUID := ""
	for _, itemLb := range userLbs {
		if foundLb := s.findLoadBalancer(lbName, userLbs, userCluster); foundLb != nil {
			err := lLoadBalancerV2.Delete(s.vLBSC, lLoadBalancerV2.NewDeleteOpts(s.getProjectID(), itemLb.UUID))
			if err != nil {
				klog.Errorf("failed to delete load balancer %s: %v", itemLb.UUID, err)
				return err
			}

			lbUUID = itemLb.UUID
			break
		}
	}

	var deleteSecgroup []string
	var validSecgroupUUIDs []string
	if len(lbUUID) > 0 {
		for _, secgroupUUID := range userCluster.MinionClusterSecGroupIDList {
			secgroup, err := lSecgroupV2.Get(s.vServerSC, lSecgroupV2.NewGetOpts(s.getProjectID(), secgroupUUID))
			if err != nil {
				klog.Warningf("failed to get secgroup %s: %v", secgroupUUID, err)
				continue
			}

			if strings.Contains(secgroup.Description, lbUUID) {
				deleteSecgroup = append(deleteSecgroup, secgroupUUID)
			} else {
				validSecgroupUUIDs = append(validSecgroupUUIDs, secgroupUUID)
			}
		}
	}

	if len(deleteSecgroup) > 0 {
		_, err := lClusterV2.UpdateSecgroup(
			s.vServerSC,
			lClusterV2.NewUpdateSecgroupOpts(
				s.getProjectID(),
				&lClusterV2.UpdateSecgroupOpts{
					ClusterID:   pClusterID,
					Master:      false,
					SecGroupIds: validSecgroupUUIDs}))

		if err != nil {
			klog.Errorf("failed to update secgroup from cluster %s: %v", pClusterID, err)
			return err
		}
	}

	userCluster, err = lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if err != nil {
		klog.Errorf("failed to get cluster %s to recheck after update secgroup: %v", pClusterID, err)
	}

	secgroupMapping := make(map[string]bool)
	for _, secgroupUUID := range userCluster.MinionClusterSecGroupIDList {
		secgroupMapping[secgroupUUID] = true
	}

	for _, secgroupUUID := range validSecgroupUUIDs {
		if _, ok := secgroupMapping[secgroupUUID]; !ok {
			klog.Errorf("secgroup %s is not existed in cluster %s after update secgroup", secgroupUUID, pClusterID)
			return fmt.Errorf("secgroup %s is not existed in cluster %s after update secgroup", secgroupUUID, pClusterID)
		}
	}

	for _, secgroupUUID := range deleteSecgroup {
		err = lSecgroupV2.Delete(s.vServerSC, lSecgroupV2.NewDeleteOpts(s.getProjectID(), secgroupUUID))
		if err != nil {
			klog.Errorf("ensureDeleteLoadBalancer; failed to delete secgroup %s: %v", secgroupUUID, err)
			return err
		}
	}

	// The loadbalancer has been deleted completely
	return nil
}

func (s *vLB) ensureGetLoadBalancer(pCtx context.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, false, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLBSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return nil, false, err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	for _, itemLb := range userLbs {
		if s.findLoadBalancer(lbName, userLbs, userCluster) != nil {
			status := &lCoreV1.LoadBalancerStatus{}
			status.Ingress = []lCoreV1.LoadBalancerIngress{{IP: itemLb.Address}}
			return status, true, nil
		}
	}

	klog.Infof("Load balancer %s is not existed", lbName)
	return nil, false, nil
}

func (s *vLB) ensureSecgroup(pLb *lLbObjV2.LoadBalancer, pCluster *lClusterObjV2.Cluster, pService *lCoreV1.Service, pPort lCoreV1.ServicePort, pServiceConfig *serviceConfig) error {
	subnet, err := lSubnetV2.Get(s.vServerSC, lSubnetV2.NewGetOpts(s.getProjectID(), pCluster.VpcID, pCluster.SubnetID))
	if err != nil {
		klog.Errorf("failed to get subnet %s: %v", pCluster.SubnetID, err)
		return err
	}

	// Create a new secgroup for this cluster to allow traffic from the loadbalancer
	secgroupName := lUtils.GenSecgroupName(pCluster.ID, pService, string(pPort.Protocol), int(pPort.Port))
	secgroupDescription := lUtils.GenSecgroupDescription(pCluster.ID, pService, pLb.UUID)
	newSecgroup, err := lSecgroupV2.Create(
		s.vServerSC,
		lSecgroupV2.NewCreateOpts(s.getProjectID(), &lSecgroupV2.CreateOpts{
			Name:        secgroupName,
			Description: secgroupDescription}))

	if err != nil {
		if lSecgroupV2.IsErrNameDuplicate(err) {
			secgroups, err := lSecgroupV2.List(s.vServerSC, lSecgroupV2.NewListOpts(s.getProjectID(), secgroupName))
			if err != nil {
				klog.Errorf("failed to list secgroup by secgroup name [%s]", secgroupName)
				return err
			}

			if len(secgroups) > 0 {
				for _, secgroup := range secgroups {
					if secgroup.Description == secgroupDescription {
						err = lSecgroupV2.Delete(s.vServerSC, lSecgroupV2.NewDeleteOpts(s.getProjectID(), secgroup.UUID))
						if err != nil {
							klog.Errorf("failed to delete secgroup %s: %v", secgroup.UUID, err)
							return err
						} else {
							return s.ensureSecgroup(pLb, pCluster, pService, pPort, pServiceConfig)
						}
					}
				}
			}
		}
		klog.Errorf("failed to create secgroup %s for cluster %s: %v", secgroupName, pCluster.ID, err)
		return err
	}

	klog.Infof("Created secgroup %s for cluster %s successfully", secgroupName, pCluster.ID)

	opts := &lSecRuleV2.CreateOpts{
		SecgroupUUID:   newSecgroup.UUID,
		Direction:      lSecRuleV2.CreateOptsDirectionOptIngress,
		EtherType:      lUtils.GetSecgroupRuleEthernetType(pServiceConfig.preferredIPFamily),
		Protocol:       lUtils.GetSecgroupRuleProtocolOpt(pPort),
		PortRangeMin:   int(pPort.Port),
		PortRangeMax:   int(pPort.Port),
		RemoteIPPrefix: subnet.CIRD}

	klog.Infof("Adding rule to secgroup %s to allow traffic from loadbalancer %s: %+v", newSecgroup.UUID, pLb.UUID, opts)
	// Add the rule to allow traffic from the loadbalancer
	_, err = lSecRuleV2.Create(s.vServerSC, lSecRuleV2.NewCreateOpts(s.getProjectID(), opts))

	if err != nil {
		klog.Errorf("failed to create secgroup rule for secgroup %s: %v", newSecgroup.UUID, err)
		return err
	}

	// Attach the secgroup to entire minion node groups
	_, err = lClusterV2.UpdateSecgroup(
		s.vServerSC,
		lClusterV2.NewUpdateSecgroupOpts(
			s.getProjectID(),
			&lClusterV2.UpdateSecgroupOpts{
				ClusterID:   pCluster.ID,
				Master:      false,
				SecGroupIds: append(pCluster.MinionClusterSecGroupIDList, newSecgroup.UUID)}))

	if err != nil {
		klog.Errorf("failed to attach secgroup %s to cluster %s: %v", newSecgroup.UUID, pCluster.ID, err)
		return err
	}

	return nil
}

func getNodeAddressForLB(node *lCoreV1.Node) (string, error) {
	addrs := node.Status.Addresses
	if len(addrs) == 0 {
		return "", errors.NewErrNodeAddressNotFound(node.Name, "")
	}

	for _, addr := range addrs {
		if addr.Type == lCoreV1.NodeInternalIP {
			return addr.Address, nil
		}
	}

	return addrs[0].Address, nil
}

func popListener(pExistingListeners []*lObjects.Listener, pNewListenerID string) []*lObjects.Listener {
	var newListeners []*lObjects.Listener

	for _, existingListener := range pExistingListeners {
		if existingListener.ID != pNewListenerID {
			newListeners = append(newListeners, existingListener)
		}
	}
	return newListeners
}
