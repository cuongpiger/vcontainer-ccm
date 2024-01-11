package vngcloud

import (
	lCtx "context"
	"fmt"
	lStr "strings"

	lCoreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	lNetUtils "k8s.io/utils/net"

	lClient "github.com/vngcloud/vcontainer-sdk/client"
	lObjects "github.com/vngcloud/vcontainer-sdk/vcontainer/objects"
	lClusterV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster"
	lClusterObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster/obj"
	lListenerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	lLoadBalancerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	lPoolV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"
	lSecgroupV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/network/v2/secgroup"

	lSdkClient "github.com/cuongpiger/vcontainer-ccm/pkg/client"
	lConsts "github.com/cuongpiger/vcontainer-ccm/pkg/consts"
	lMetrics "github.com/cuongpiger/vcontainer-ccm/pkg/metrics"
	lUtils "github.com/cuongpiger/vcontainer-ccm/pkg/utils"
	lErrors "github.com/cuongpiger/vcontainer-ccm/pkg/utils/errors"
	lMetadata "github.com/cuongpiger/vcontainer-ccm/pkg/utils/metadata"
)

type (
	// VLbOpts is default vLB configurations that are loaded from the vcontainer-ccm config file
	VLbOpts struct {
		DefaultL4PackageID               string `gcfg:"default-l4-package-id"`
		DefaultListenerAllowedCIDRs      string `gcfg:"default-listener-allowed-cidrs"`
		DefaultIdleTimeoutClient         int    `gcfg:"default-idle-timeout-client"`
		DefaultIdleTimeoutMember         int    `gcfg:"default-idle-timeout-member"`
		DefaultIdleTimeoutConnection     int    `gcfg:"default-idle-timeout-connection"`
		DefaultPoolAlgorithm             string `gcfg:"default-pool-algorithm"`
		DefaultPoolProtocol              string `gcfg:"default-pool-protocol"`
		DefaultMonitorProtocol           string `gcfg:"default-monitor-protocol"`
		DefaultMonitorHealthyThreshold   int    `gcfg:"default-monitor-healthy-threshold"`
		DefaultMonitorUnhealthyThreshold int    `gcfg:"default-monitor-unhealthy-threshold"`
		DefaultMonitorTimeout            int    `gcfg:"default-monitor-timeout"`
		DefaultMonitorInterval           int    `gcfg:"default-monitor-interval"`
		DefaultMonitorHttpMethod         string `gcfg:"default-monitor-http-method"`
		DefaultMonitorHttpPath           string `gcfg:"default-monitor-http-path"`
		DefaultMonitorHttpSuccessCode    string `gcfg:"default-monitor-http-success-code"`
		DefaultMonitorHttpVersion        string `gcfg:"default-monitor-http-version"`
		DefaultMonitorHttpDomainName     string `gcfg:"default-monitor-http-domain-name"`
	}

	// vLB is the implementation of the VNG CLOUD for actions on load balancer
	vLB struct {
		vLbSC     *lClient.ServiceClient
		vServerSC *lClient.ServiceClient

		kubeClient    kubernetes.Interface
		eventRecorder record.EventRecorder
		vLbConfig     VLbOpts
		extraInfo     *ExtraInfo
	}

	// Config is the configuration for the VNG CLOUD load balancer controller,
	// it is loaded from the vcontainer-ccm config file
	Config struct {
		Global   lSdkClient.AuthOpts // global configurations, it is loaded from Helm helpers and values.yaml
		VLB      VLbOpts             // vLB configurations, it is loaded from Helm helpers and values.yaml
		Metadata lMetadata.Opts      // metadata service config, by default is empty
	}
)

type (
	serviceConfig struct {
		internal                  bool
		lbID                      string
		preferredIPFamily         lCoreV1.IPFamily // preferred (the first) IP family indicated in service's `spec.ipFamilies`
		flavorID                  string
		lbType                    lLoadBalancerV2.CreateOptsTypeOpt
		projectID                 string
		subnetID                  string
		isOwner                   bool
		allowedCIRDs              string
		idleTimeoutClient         int
		idleTimeoutMember         int
		idleTimeoutConnection     int
		poolAlgorithm             string
		poolProtocol              string
		monitorProtocol           string
		monitorHealthyThreshold   int
		monitorUnhealthyThreshold int
		monitorInterval           int
		monitorTimeout            int
		monitorHTTPMethod         string
		monitorHTTPPath           string
		monitorHTTPSuccessCode    string
		monitorHttpVersion        string
		monitorHttpDomainName     string
	}
	listenerKey struct {
		Protocol string
		Port     int
	}
)

// ****************************** IMPLEMENTATIONS OF KUBERNETES CLOUD PROVIDER INTERFACE *******************************

func (s *vLB) GetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("GetLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, existed, err := s.ensureGetLoadBalancer(pCtx, pClusterID, pService)
	return status, existed, mc.ObserveReconcile(err)
}

func (s *vLB) GetLoadBalancerName(_ lCtx.Context, pClusterName string, pService *lCoreV1.Service) string {
	genName := lUtils.GenLoadBalancerName(pClusterName, pService)
	lbPrefixName := lUtils.GenLoadBalancerPrefixName(pClusterName)
	lbName := getStringFromServiceAnnotation(pService, ServiceAnnotationLoadBalancerName, "")

	if len(lbName) > 0 {
		lbName = fmt.Sprintf("%s-%s", lbPrefixName, lbName)
		return lbName[:lUtils.MinInt(len(lbName), lConsts.DEFAULT_PORTAL_NAME_LENGTH)]
	} else {
		return genName
	}
}

func (s *vLB) EnsureLoadBalancer(
	pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) (*lCoreV1.LoadBalancerStatus, error) {

	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancer", "cluster", pClusterID, "service", klog.KObj(pService))
	status, err := s.ensureLoadBalancer(pCtx, pClusterID, pService, pNodes)
	return status, mc.ObserveReconcile(err)
}

func (s *vLB) UpdateLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) error {
	klog.Infof("UpdateLoadBalancer: update load balancer for service %s/%s, the nodes are: %v", pService.Namespace, pService.Name, pNodes)
	return nil
}

func (s *vLB) EnsureLoadBalancerDeleted(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {
	mc := lMetrics.NewMetricContext("loadbalancer", "ensure")
	klog.InfoS("EnsureLoadBalancerDeleted", "cluster", pClusterID, "service", klog.KObj(pService))
	err := s.ensureDeleteLoadBalancer(pCtx, pClusterID, pService)
	return mc.ObserveReconcile(err)
}

// ************************************************** PRIVATE METHODS **************************************************

func (s *vLB) ensureLoadBalancer(
	pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service, pNodes []*lCoreV1.Node) ( // params
	rLb *lCoreV1.LoadBalancerStatus, rErr error) { // returns

	var (
		userLb               *lObjects.LoadBalancer   // hold the loadbalancer that user want to reuse or create
		lsLbs                []*lObjects.LoadBalancer // hold the list of loadbalancer existed in the project of this cluster
		lbListeners          []*lObjects.Listener     // hold the list of listeners of the loadbalancer attach to this cluster
		svcAnnotations       map[string]string
		createNewLb, isOwner bool // check the loadbalancer is created and this cluster is the owner

		svcConf            = new(serviceConfig)                                // the lb configurations CAN be applied to create or update
		curListenerMapping = make(map[listenerKey]*lObjects.Listener)          // this use to check conflict port and protocol
		lbName             = s.GetLoadBalancerName(pCtx, pClusterID, pService) // hold the loadbalancer name
	)

	// Patcher the service to prevent the service is updated by other controller
	patcher := newServicePatcher(s.kubeClient, pService)
	defer func() {
		rErr = patcher.Patch(pCtx, rErr)
	}()

	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if err != nil || userCluster == nil {
		klog.Warningf("cluster %s NOT found, please check the secret resource in the Helm template", pClusterID)
		return nil, err
	}

	// Set the default configurations for loadbalancer into the 'svcConf' variable
	if err = s.configureLoadBalancerParams(pService, pNodes, svcConf, userCluster); err != nil {
		klog.Errorf("failed to configure load balancer params for service %s/%s: %v", pService.Namespace, pService.Name, err)
		return nil, err
	}

	if len(svcConf.lbID) > 0 {
		// User uses Service Annotation to reuse the load balancer
		userLb, err = lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), svcConf.lbID))
		if err != nil || userLb == nil {
			klog.Errorf("failed to get load balancer %s for service %s/%s: %v", svcConf.lbID, pService.Namespace, pService.Name, err)
			return nil, err
		}

		if svcConf.internal != userLb.Internal {
			klog.Errorf("the loadbalancer type of the original loadbalancer and service file are not match")
			return nil, lErrors.NewErrConflictServiceAndCloud("the loadbalancer type of the original loadbalancer and service file are not match")
		}

		createNewLb = false
		svcAnnotations[ServiceAnnotationLoadBalancerID] = svcConf.lbID
	} else {
		// User want you to create a new loadbalancer for this service
		klog.V(5).Infof("did not specify load balancer ID, maybe creating a new one")
		isOwner = true
		createNewLb = true
	}

	// If until this step, can not find any load balancer for this cluster, find entire the project
	if lsLbs, err = lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID())); err != nil {
		klog.Errorf("failed to find load balancers for cluster %s: %v", pClusterID, err)
		return nil, err
	}

	userLb = s.findLoadBalancer(lbName, lsLbs, userCluster)
	isOwner = lUtils.CheckOwner(userCluster, userLb, pService)
	createNewLb = userLb == nil

	if isOwner && createNewLb {
		// The loadbalancer is created and this cluster is the owner
		if userLb, err = s.createLoadBalancer(lbName, pService, svcConf); err != nil {
			klog.Errorf("failed to create load balancer for service %s/%s: %v", pService.Namespace, pService.Name, err)
			return nil, err
		}

		svcAnnotations[serviceAnnotionOwnerClusterID] = userCluster.ID
		svcAnnotations[ServiceAnnotationLoadBalancerID] = userLb.UUID
	}

	// Check ports are conflict or not
	if lbListeners, err = lListenerV2.GetBasedLoadBalancer(
		s.vLbSC, lListenerV2.NewGetBasedLoadBalancerOpts(s.getProjectID(), userLb.UUID)); err != nil {
		klog.Errorf("failed to get listeners for load balancer %s: %v", userLb.UUID, err)
		return nil, err
	}

	// If this loadbalancer has some listeners
	if lbListeners != nil && len(lbListeners) > 0 {
		// Loop via the listeners of this loadbalancer to get the mapping of port and protocol
		for i, itemListener := range lbListeners {
			key := listenerKey{Protocol: itemListener.Protocol, Port: itemListener.ProtocolPort}
			curListenerMapping[key] = lbListeners[i]
		}

		// Check the port and protocol conflict on the listeners of this loadbalancer
		if err = s.checkListeners(pService, curListenerMapping, userCluster, userLb); err != nil {
			klog.Errorf("the port and protocol is conflict: %v", err)
			return nil, err
		}
	}

	// Ensure pools and listener for this loadbalancer
	klog.V(5).Infof("processing listeners and pools")
	for _, port := range pService.Spec.Ports {
		// Ensure pools
		newPool, err := s.ensurePool(userCluster, userLb, pService, port, pNodes, svcConf)
		if err != nil {
			klog.Errorf("failed to create pool for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		// Ensure listners
		_, err = s.ensureListener(userCluster, userLb.UUID, newPool.UUID, lbName, port, pService, svcConf)
		if err != nil {
			klog.Errorf("failed to create listener for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}

		klog.Infof("created listener and pool for load balancer %s successfully", userLb.UUID)
	}
	klog.V(5).Infof("processing listeners and pools completely")

	for _, itemListener := range lbListeners {
		err = lListenerV2.Delete(s.vLbSC, lListenerV2.NewDeleteOpts(s.getProjectID(), userLb.UUID, itemListener.ID))
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

	if getBoolFromServiceAnnotation(pService, ServiceAnnotationEnableSecgroupDefault, true) {
		klog.V(5).Infof("processing security group")
		// Update the security group
		if err = s.ensureSecgroup(userCluster); err != nil {
			klog.Errorf("failed to update security group for load balancer %s: %v", userLb.UUID, err)
			return nil, err
		}
	}

	// Update the annatation for the K8s service
	s.updateK8sServiceAnnotations(pService, svcAnnotations)

	klog.V(5).Infof("processing load balancer status")
	lbStatus := s.createLoadBalancerStatus(userLb.Address)

	klog.Infof(
		"load balancer %s for service %s/%s is ready to use for Kubernetes controller",
		lbName, pService.Namespace, pService.Name)
	return lbStatus, nil
}

func (s *vLB) createLoadBalancer(pLbName string, pService *lCoreV1.Service, pServiceConfig *serviceConfig) ( // params
	*lObjects.LoadBalancer, error) { // returns

	opts := &lLoadBalancerV2.CreateOpts{
		Scheme:    lUtils.ParseLoadBalancerScheme(pServiceConfig.internal),
		Type:      pServiceConfig.lbType,
		Name:      pLbName,
		PackageID: pServiceConfig.flavorID,
		SubnetID:  pServiceConfig.subnetID,
	}

	klog.Infof("creating load balancer %s for service %s", pLbName, pService.Name)
	mc := lMetrics.NewMetricContext("loadbalancer", "create")
	newLb, err := lLoadBalancerV2.Create(s.vLbSC, lLoadBalancerV2.NewCreateOpts(s.extraInfo.ProjectID, opts))

	if mc.ObserveReconcile(err) != nil {
		klog.Errorf("failed to create load balancer %s for service %s: %v", pLbName, pService.Name, err)
		return nil, err
	}

	klog.Infof("created load balancer %s for service %s, waiting for ready", newLb.UUID, pService.Name)
	newLb, err = s.waitLoadBalancerReady(newLb.UUID)
	if err != nil {
		klog.Errorf("failed to wait load balancer %s for service %s, now try to delete it: %v", newLb.UUID, pService.Name, err)
		// delete this loadbalancer
		if err2 := lLoadBalancerV2.Delete(s.vLbSC, lLoadBalancerV2.NewDeleteOpts(s.extraInfo.ProjectID, newLb.UUID)); err2 != nil {
			klog.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
			return nil, fmt.Errorf("failed to delete load balancer %s for service %s after creating timeout: %v", newLb.UUID, pService.Name, err2)
		}

		return nil, err
	}

	klog.Infof("created load balancer %s for service %s successfully", newLb.UUID, pService.Name)
	return newLb, nil
}

func (s *vLB) waitLoadBalancerReady(pLbID string) (*lObjects.LoadBalancer, error) {

	klog.Infof("Waiting for load balancer %s to be ready", pLbID)
	var resultLb *lObjects.LoadBalancer

	err := wait.ExponentialBackoff(wait.Backoff{
		Duration: waitLoadbalancerInitDelay,
		Factor:   waitLoadbalancerFactor,
		Steps:    waitLoadbalancerActiveSteps,
	}, func() (done bool, err error) {
		mc := lMetrics.NewMetricContext("loadbalancer", "get")
		lb, err := lLoadBalancerV2.Get(s.vLbSC, lLoadBalancerV2.NewGetOpts(s.getProjectID(), pLbID))
		if mc.ObserveReconcile(err) != nil {
			klog.Errorf("failed to get load balancer %s: %v", pLbID, err)
			return false, err
		}

		if lStr.ToUpper(lb.Status) == ACTIVE_LOADBALANCER_STATUS {
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

func (s *vLB) ensurePool(
	pCluster *lClusterObjV2.Cluster, pLb *lObjects.LoadBalancer, pService *lCoreV1.Service, pPort lCoreV1.ServicePort,
	pNodes []*lCoreV1.Node, pServiceConfig *serviceConfig) (*lObjects.Pool, error) {

	// Get the pool name of the service
	poolName := lUtils.GenListenerAndPoolName(pCluster.ID, pService)

	// Get all pools of this loadbalancer based on the load balancer uuid
	pools, err := lPoolV2.ListPoolsBasedLoadBalancer(
		s.vLbSC, lPoolV2.NewListPoolsBasedLoadBalancerOpts(s.extraInfo.ProjectID, pLb.UUID))

	if err != nil {
		klog.Errorf("failed to list pools for load balancer %s: %v", pLb.UUID, err)
		return nil, err
	}

	// Loop via pool
	for _, itemPool := range pools {
		// If this pool is not valid
		if !lUtils.IsPoolProtocolValid(itemPool, pPort, poolName) {
			err = lPoolV2.Delete(s.vLbSC, lPoolV2.NewDeleteOpts(s.extraInfo.ProjectID, pLb.UUID, itemPool.UUID))
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

	poolMembers, err := prepareMembers(pNodes, pPort, pServiceConfig)
	if err != nil {
		klog.Errorf("failed to prepare members for pool %s: %v", pService.Name, err)
		return nil, err
	}

	poolProtocol := lUtils.ParsePoolProtocol(pServiceConfig.poolProtocol)
	newPool, err := lPoolV2.Create(s.vLbSC, lPoolV2.NewCreateOpts(s.extraInfo.ProjectID, pLb.UUID, &lPoolV2.CreateOpts{
		Algorithm:    lUtils.ParsePoolAlgorithm(pServiceConfig.poolAlgorithm),
		PoolName:     poolName,
		PoolProtocol: poolProtocol,
		Members:      poolMembers,
		HealthMonitor: lPoolV2.HealthMonitor{
			HealthCheckProtocol: lUtils.ParseMonitorProtocol(pServiceConfig.monitorProtocol),
			HealthCheckPath:     pServiceConfig.monitorHTTPPath,
			HealthyThreshold:    pServiceConfig.monitorHealthyThreshold,
			UnhealthyThreshold:  pServiceConfig.monitorUnhealthyThreshold,
			Timeout:             pServiceConfig.monitorTimeout,
			Interval:            pServiceConfig.monitorInterval,
			DomainName:          pServiceConfig.monitorHttpDomainName,
			HttpVersion:         pServiceConfig.monitorHttpVersion,
			SuccessCode:         pServiceConfig.monitorHTTPSuccessCode,
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

func (s *vLB) ensureListener(
	pCluster *lClusterObjV2.Cluster, pLbID, pPoolID, pLbName string, pPort lCoreV1.ServicePort, // params
	pService *lCoreV1.Service, pServiceConfig *serviceConfig) ( // params
	*lObjects.Listener, error) { // returns

	mc := lMetrics.NewMetricContext("listener", "create")
	newListener, err := lListenerV2.Create(s.vLbSC, lListenerV2.NewCreateOpts(
		s.extraInfo.ProjectID,
		pLbID,
		&lListenerV2.CreateOpts{
			AllowedCidrs:         pServiceConfig.allowedCIRDs,
			DefaultPoolId:        pPoolID,
			ListenerName:         lUtils.GenListenerAndPoolName(pCluster.ID, pService),
			ListenerProtocol:     lUtils.ParseListenerProtocol(pPort),
			ListenerProtocolPort: int(pPort.Port),
			TimeoutClient:        pServiceConfig.idleTimeoutClient,
			TimeoutMember:        pServiceConfig.idleTimeoutMember,
			TimeoutConnection:    pServiceConfig.idleTimeoutConnection,
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

func (s *vLB) ensureDeleteLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) error {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
	if err != nil {
		klog.Warningf("failed to list load balancers for cluster %s in the subnet %s: %v", pClusterID, userCluster.SubnetID, err)
		return err
	}

	// Get the loadbalancer by subnetID and loadbalancer name
	lbName := s.GetLoadBalancerName(pCtx, pClusterID, pService)
	for _, itemLb := range userLbs {
		if s.findLoadBalancer(lbName, userLbs, userCluster) != nil {
			// Delete this loadbalancer
			klog.V(5).Infof("deleting load balancer [UUID:%s]", itemLb.UUID)
			err := lLoadBalancerV2.Delete(s.vLbSC, lLoadBalancerV2.NewDeleteOpts(s.getProjectID(), itemLb.UUID))
			if err != nil {
				klog.Errorf("failed to delete load balancer [UUID:%s]: %v", itemLb.UUID, err)
				return err
			}

			klog.V(5).Infof("deleted load balancer [UUID:%s] successfully", itemLb.UUID)
			break
		}
	}

	// The loadbalancer has been deleted completely
	return nil
}

func (s *vLB) ensureGetLoadBalancer(pCtx lCtx.Context, pClusterID string, pService *lCoreV1.Service) (*lCoreV1.LoadBalancerStatus, bool, error) {
	// Get the cluster info from the cluster ID that user provided from the K8s secret
	userCluster, err := lClusterV2.Get(s.vServerSC, lClusterV2.NewGetOpts(s.getProjectID(), pClusterID))
	if lClusterV2.IsErrClusterNotFound(err) {
		klog.Warningf("cluster %s not found, please check the secret resource in the Helm template", pClusterID)
		return nil, false, err
	}

	// Get this loadbalancer name
	userLbs, err := lLoadBalancerV2.List(s.vLbSC, lLoadBalancerV2.NewListOpts(s.getProjectID()))
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

func (s *vLB) ensureSecgroup(pCluster *lClusterObjV2.Cluster) error {
	defaultSecgroup, err := s.findDefaultSecgroup()
	if err != nil {
		klog.Errorf("failed to find default secgroup: %v", err)
		return err
	}

	alreadyAttach := false
	for _, secgroup := range pCluster.MinionClusterSecGroupIDList {
		if secgroup == defaultSecgroup.UUID {
			alreadyAttach = true
			break
		}
	}

	if !alreadyAttach {
		lstSecgroupID := append(pCluster.MinionClusterSecGroupIDList, defaultSecgroup.UUID)
		klog.Infof("Attaching default secgroup %v to cluster %s", lstSecgroupID, pCluster.ID)
		// Attach the secgroup to entire minion node groups
		_, err = lClusterV2.UpdateSecgroup(
			s.vServerSC,
			lClusterV2.NewUpdateSecgroupOpts(
				s.getProjectID(),
				&lClusterV2.UpdateSecgroupOpts{
					ClusterID:   pCluster.ID,
					Master:      false,
					SecGroupIds: lstSecgroupID}))

		if err != nil {
			klog.Errorf("failed to attach secgroup %s to cluster %s: %v", defaultSecgroup.UUID, pCluster.ID, err)
			return err
		}
	}

	klog.Infof("Attached default secgroup %s to cluster %s successfully", defaultSecgroup.UUID, pCluster.ID)

	return nil
}

func (s *vLB) findDefaultSecgroup() (*lObjects.Secgroup, error) {
	secgroups, err := lSecgroupV2.List(s.vServerSC, lSecgroupV2.NewListOpts(s.getProjectID(), lConsts.DEFAULT_SECGROP_NAME))
	if err != nil {
		klog.Errorf("failed to list secgroup by secgroup name [%s]", lConsts.DEFAULT_SECGROP_NAME)
		return nil, err
	}

	if len(secgroups) < 1 {
		klog.Errorf("the project [%s] has no default secgroup", s.getProjectID())
		return nil, lErrors.NewErrNoDefaultSecgroup(s.getProjectID(), "")
	}

	for _, secgroup := range secgroups {
		if lStr.TrimSpace(lStr.ToLower(secgroup.Description)) == lConsts.DEFAULT_SECGROUP_DESCRIPTION && secgroup.Name == lConsts.DEFAULT_SECGROP_NAME {
			return secgroup, nil
		}
	}

	klog.Errorf("the project [%s] has no default secgroup", s.getProjectID())
	return nil, lErrors.NewErrNoDefaultSecgroup(s.getProjectID(), "")
}

func (s *vLB) configureLoadBalancerParams(pService *lCoreV1.Service, pNodes []*lCoreV1.Node, // params
	pServiceConfig *serviceConfig, pCluster *lClusterObjV2.Cluster) error { // returns

	// Check if there is any node available
	workerNodes := lUtils.ListWorkerNodes(pNodes, true)
	if len(workerNodes) < 1 {
		klog.Errorf("no worker node available for this cluster")
		return lErrors.NewNoNodeAvailable()
	}

	// Check if the service spec has any port, if not, return error
	ports := pService.Spec.Ports
	if len(ports) <= 0 {
		return lErrors.NewErrServicePortEmpty()
	}

	// Since the plugin does not support multiple load-balancers per service yet, the first IP family will determine the IP family of the load-balancer
	if len(pService.Spec.IPFamilies) > 0 {
		pServiceConfig.preferredIPFamily = pService.Spec.IPFamilies[0]
	}

	// Get the loadbalancer ID from the service annotation, default is empty
	pServiceConfig.lbID = getStringFromServiceAnnotation(
		pService, ServiceAnnotationLoadBalancerID, "")

	// Check if user want to create an internal load-balancer
	pServiceConfig.internal = getBoolFromServiceAnnotation(
		pService, ServiceAnnotationLoadBalancerInternal, false)

	// If the service is IPv6 family, the load-balancer must be internal
	if pServiceConfig.preferredIPFamily == lCoreV1.IPv6Protocol {
		pServiceConfig.internal = true
	}

	// Get the subnet ID from the cluster
	pServiceConfig.subnetID = pCluster.SubnetID

	// Set option loadbalancer type is Layer 4 in the request option
	pServiceConfig.lbType = lLoadBalancerV2.CreateOptsTypeOptLayer4

	// Get the flavor ID from the service annotation, default is get from the cloud config file
	pServiceConfig.flavorID = getStringFromServiceAnnotation(
		pService, ServiceAnnotationPackageID, s.vLbConfig.DefaultL4PackageID)

	// Get the allowed CIDRs from the service annotation, default is get from the cloud config file
	pServiceConfig.allowedCIRDs = getStringFromServiceAnnotation(
		pService, ServiceAnnotationListenerAllowedCIDRs, s.vLbConfig.DefaultListenerAllowedCIDRs)

	// Set default for the idle timeout
	pServiceConfig.idleTimeoutClient = getIntFromServiceAnnotation(
		pService, ServiceAnnotationIdleTimeoutClient, s.vLbConfig.DefaultIdleTimeoutClient)

	pServiceConfig.idleTimeoutMember = getIntFromServiceAnnotation(
		pService, ServiceAnnotationIdleTimeoutMember, s.vLbConfig.DefaultIdleTimeoutMember)

	pServiceConfig.idleTimeoutConnection = getIntFromServiceAnnotation(
		pService, ServiceAnnotationIdleTimeoutConnection, s.vLbConfig.DefaultIdleTimeoutConnection)

	// Set the pool algorithm, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.poolAlgorithm = getStringFromServiceAnnotation(
		pService, ServiceAnnotationPoolAlgorithm, s.vLbConfig.DefaultPoolAlgorithm)

	// Set the pool protocol, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.poolProtocol = getStringFromServiceAnnotation(
		pService, ServiceAnnotationPoolProtocol, s.vLbConfig.DefaultPoolProtocol)

	// Set the pool healthcheck protocol
	pServiceConfig.monitorProtocol = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorProtocol, s.vLbConfig.DefaultMonitorProtocol)

	// Set the pool healthcheck options
	pServiceConfig.monitorHealthyThreshold = getIntFromServiceAnnotation(
		pService, ServiceAnnotationHealthyThreshold, s.vLbConfig.DefaultMonitorHealthyThreshold)

	// Set the monitor unhealthy threshold, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorUnhealthyThreshold = getIntFromServiceAnnotation(
		pService, ServiceAnnotationMonitorUnhealthyThreshold, s.vLbConfig.DefaultMonitorUnhealthyThreshold)

	// Set the monitor timeout, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorTimeout = getIntFromServiceAnnotation(
		pService, ServiceAnnotationMonitorTimeout, s.vLbConfig.DefaultMonitorTimeout)

	// Set the monitor interval, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorInterval = getIntFromServiceAnnotation(
		pService, ServiceAnnotationMonitorInterval, s.vLbConfig.DefaultMonitorInterval)

	// Set the monitor http method, default gets from config file if not set in the service annotation
	pServiceConfig.monitorHTTPMethod = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorHTTPMethod, s.vLbConfig.DefaultMonitorHttpMethod)

	// Set the http monitor path, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorHTTPPath = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorHTTPPath, s.vLbConfig.DefaultMonitorHttpPath)

	// Set the http monitor success code, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorHTTPSuccessCode = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorHTTPSuccessCode, s.vLbConfig.DefaultMonitorHttpSuccessCode)

	// Set the http monitor version, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorHttpVersion = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorHTTPVersion, s.vLbConfig.DefaultMonitorHttpVersion)

	// Set the http monitor domain name, default is get from the cloud config file if not set in the service annotation
	pServiceConfig.monitorHttpDomainName = getStringFromServiceAnnotation(
		pService, ServiceAnnotationMonitorHTTPDomainName, s.vLbConfig.DefaultMonitorHttpDomainName)

	return nil
}

func (s *vLB) findLoadBalancer(pLbName string, pLbs []*lObjects.LoadBalancer, // params
	pCluster *lClusterObjV2.Cluster) *lObjects.LoadBalancer { // returns

	if pCluster == nil || len(pLbs) < 1 {
		return nil
	}

	for _, lb := range pLbs {
		if lb.SubnetID == pCluster.SubnetID && lb.Name == pLbName {
			return lb
		}
	}

	return nil
}

// checkListeners checks if there is conflict for ports.
func (s *vLB) checkListeners(
	pService *lCoreV1.Service, pListenerMapping map[listenerKey]*lObjects.Listener, // params
	pCluster *lClusterObjV2.Cluster, pLb *lObjects.LoadBalancer) error { // params and returns

	for _, svcPort := range pService.Spec.Ports {
		key := listenerKey{Protocol: string(svcPort.Protocol), Port: int(svcPort.Port)}
		listenerName := lUtils.GenListenerAndPoolName(pCluster.ID, pService)

		if lbListener, isPresent := pListenerMapping[key]; isPresent {
			if lbListener.Name != listenerName {
				klog.Errorf("the port %d and protocol %s is conflict", svcPort.Port, svcPort.Protocol)
				return lErrors.NewErrConflictService(int(svcPort.Port), string(svcPort.Protocol), "")
			}

			klog.Infof("the port %d and protocol %s is already existed", svcPort.Port, svcPort.Protocol)
		} else if lbListener.Name == listenerName {
			// If the listener is already existed, but the port and protocol is not correct
			klog.Errorf("the port %d and protocol %s is conflict", svcPort.Port, svcPort.Protocol)
			poolID := lbListener.DefaultPoolUUID

			if err := lPoolV2.Delete(
				s.vLbSC, lPoolV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, poolID)); err != nil {
				klog.Errorf("failed to delete pool %s for load balancer %s: %v", poolID, pLb.UUID, err)
				return err
			}

			klog.V(5).Infof("waiting to delete pool %s for load balancer %s complete", poolID, pLb.UUID)
			if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
				klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
				return err
			}

			if err := lListenerV2.Delete(
				s.vLbSC, lListenerV2.NewDeleteOpts(s.getProjectID(), pLb.UUID, lbListener.ID)); err != nil {
				klog.Errorf("failed to delete listener %s for load balancer %s: %v", lbListener.ID, pLb.UUID, err)
				return err
			}

			klog.V(5).Infof("waiting to delete listener %s for load balancer %s complete", lbListener.ID, pLb.UUID)
			if _, err := s.waitLoadBalancerReady(pLb.UUID); err != nil {
				klog.Errorf("failed to wait load balancer %s for service %s: %v", pLb.UUID, pService.Name, err)
				return err
			}
		}
	}

	klog.Infof("the port and protocol is not conflict")
	return nil
}

func (s *vLB) updateK8sServiceAnnotations(pService *lCoreV1.Service, pNewAnnotations map[string]string) {
	if pService.ObjectMeta.Annotations == nil {
		pService.ObjectMeta.Annotations = map[string]string{}
	}

	for key, value := range pNewAnnotations {
		pService.ObjectMeta.Annotations[key] = value
	}
}

// ********************************************* DIRECTLY SUPPORT FUNCTIONS ********************************************

func nodeAddressForLB(node *lCoreV1.Node, preferredIPFamily lCoreV1.IPFamily) (string, error) {
	addrs := node.Status.Addresses
	if len(addrs) == 0 {
		return "", lErrors.NewErrNodeAddressNotFound(node.Name, "")
	}

	allowedAddrTypes := []lCoreV1.NodeAddressType{lCoreV1.NodeInternalIP, lCoreV1.NodeExternalIP}

	for _, allowedAddrType := range allowedAddrTypes {
		for _, addr := range addrs {
			if addr.Type == allowedAddrType {
				switch preferredIPFamily {
				case lCoreV1.IPv4Protocol:
					if lNetUtils.IsIPv4String(addr.Address) {
						return addr.Address, nil
					}
				case lCoreV1.IPv6Protocol:
					if lNetUtils.IsIPv6String(addr.Address) {
						return addr.Address, nil
					}
				default:
					return addr.Address, nil
				}
			}
		}
	}

	return "", lErrors.NewErrNodeAddressNotFound(node.Name, "")
}

func getNodeAddressForLB(pNode *lCoreV1.Node) (string, error) {
	addrs := pNode.Status.Addresses
	if len(addrs) == 0 {
		return "", lErrors.NewErrNodeAddressNotFound(pNode.Name, "")
	}

	for _, addr := range addrs {
		if addr.Type == lCoreV1.NodeInternalIP {
			return addr.Address, nil
		}
	}

	return addrs[0].Address, nil
}

func prepareMembers(pNodes []*lCoreV1.Node, pPort lCoreV1.ServicePort, pServiceConfig *serviceConfig) ( // params
	[]lPoolV2.Member, error) { // returns

	var poolMembers []lPoolV2.Member
	workerNodes := lUtils.ListWorkerNodes(pNodes, true)

	if len(workerNodes) < 1 {
		klog.Errorf("no worker node available for this cluster")
		return nil, lErrors.NewNoNodeAvailable()
	}

	for _, itemNode := range workerNodes {
		_, err := getNodeAddressForLB(itemNode)
		if err != nil {
			if lErrors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

		nodeAddress, err := nodeAddressForLB(itemNode, pServiceConfig.preferredIPFamily)
		if err != nil {
			if lErrors.IsErrNodeAddressNotFound(err) {
				klog.Warningf("failed to get address for node %s: %v", itemNode.Name, err)
				continue
			} else {
				return nil, fmt.Errorf("failed to get address for node %s: %v", itemNode.Name, err)
			}
		}

		// It's 0 when AllocateLoadBalancerNodePorts=False
		if pPort.NodePort != 0 {
			poolMembers = append(poolMembers, lPoolV2.Member{
				Backup:      lConsts.DEFAULT_MEMBER_BACKUP_ROLE,
				IpAddress:   nodeAddress,
				Port:        int(pPort.NodePort),
				Weight:      lConsts.DEFAULT_MEMBER_WEIGHT,
				MonitorPort: int(pPort.NodePort),
			})
		}
	}

	return poolMembers, nil
}
