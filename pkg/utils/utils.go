package utils

import (
	"context"
	"encoding/json"
	"fmt"
	lStr "strings"
	"time"

	lCoreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"

	lConsts "github.com/cuongpiger/vcontainer-ccm/pkg/consts"

	lObjects "github.com/vngcloud/vcontainer-sdk/vcontainer/objects"
	lClusterObjV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/coe/v2/cluster/obj"
	lListenerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	lLoadBalancerV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/loadbalancer"
	lPoolV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"
)

type MyDuration struct {
	time.Duration
}

// PatchService makes patch request to the Service object.
func PatchService(ctx context.Context, client clientset.Interface, cur, mod *lCoreV1.Service) error {
	curJSON, err := json.Marshal(cur)
	if err != nil {
		return fmt.Errorf("failed to serialize current service object: %s", err)
	}

	modJSON, err := json.Marshal(mod)
	if err != nil {
		return fmt.Errorf("failed to serialize modified service object: %s", err)
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(curJSON, modJSON, lCoreV1.Service{})
	if err != nil {
		return fmt.Errorf("failed to create 2-way merge patch: %s", err)
	}

	if len(patch) == 0 || string(patch) == "{}" {
		return nil
	}

	_, err = client.CoreV1().Services(cur.Namespace).Patch(ctx, cur.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch service object %s/%s: %s", cur.Namespace, cur.Name, err)
	}

	return nil
}

func IsPoolProtocolValid(pPool *lObjects.Pool, pPort lCoreV1.ServicePort, pPoolName string) bool {
	if pPool != nil &&
		lStr.ToUpper(lStr.TrimSpace(pPool.Protocol)) != lStr.ToUpper(lStr.TrimSpace(string(pPort.Protocol))) &&
		pPoolName == pPool.Name {
		return false
	}

	return true
}

func GenListenerAndPoolName(pClusterID string, pService *lCoreV1.Service, pPort lCoreV1.ServicePort) string {
	portSuffix := fmt.Sprintf("-%s-%d", pPort.Protocol, pPort.Port)
	serviceName := GenLoadBalancerName(pClusterID, pService)
	if lConsts.DEFAULT_PORTAL_NAME_LENGTH-len(serviceName)-len(portSuffix) >= 0 {
		return lStr.ToLower(serviceName + portSuffix)
	}

	delta := lConsts.DEFAULT_PORTAL_NAME_LENGTH - len(portSuffix)
	return lStr.ToLower(serviceName[:delta] + portSuffix)
}

/*
GenLoadBalancerName generates a load balancer name from a cluster ID and a service. The length of the name is limited to
50 characters. The format of the name is: clu-<cluster_id>-<service_name>_<namespace>.

PARAMS:
- pClusterID: cluster ID
- pService: service object
RETURN:
- load balancer name
*/
func GenLoadBalancerName(pClusterID string, pService *lCoreV1.Service) string {
	lbName := fmt.Sprintf(
		"%s-%s_%s",
		GenLoadBalancerPrefixName(pClusterID),
		pService.Namespace, pService.Name)
	return lStr.ToLower(lbName[:MinInt(len(lbName), lConsts.DEFAULT_PORTAL_NAME_LENGTH)])
}

func GenLoadBalancerPrefixName(pClusterID string) string {
	lbName := fmt.Sprintf(
		"%s-%s",
		lConsts.DEFAULT_LB_PREFIX_NAME,
		pClusterID[lConsts.DEFAULT_VLB_ID_PIECE_START_INDEX:lConsts.DEFAULT_VLB_ID_PIECE_START_INDEX+lConsts.DEFAULT_VLB_ID_PIECE_LENGTH])
	return lbName
}
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func ListWorkerNodes(pNodes []*lCoreV1.Node, pOnlyReadyNode bool) []*lCoreV1.Node {
	var workerNodes []*lCoreV1.Node

	for _, node := range pNodes {
		// Ignore master nodes
		if _, ok := node.GetObjectMeta().GetLabels()[lConsts.DEFAULT_K8S_MASTER_LABEL]; ok {
			continue
		}

		// If the status of node is not considered, add it to the list
		if !pOnlyReadyNode {
			workerNodes = append(workerNodes, node)
			continue
		}

		// If this worker does not have any condition, ignore it
		if len(node.Status.Conditions) < 1 {
			continue
		}

		// Only consider ready nodes
		for _, condition := range node.Status.Conditions {
			if condition.Type == lCoreV1.NodeReady && condition.Status != lCoreV1.ConditionTrue {
				continue
			}
		}

		// This is a truly well worker, add it to the list
		workerNodes = append(workerNodes, node)
	}

	return workerNodes
}

func CheckOwner(pCluster *lClusterObjV2.Cluster, pLb *lObjects.LoadBalancer, pService *lCoreV1.Service) bool {
	if pLb == nil {
		return true
	}

	svcAnnotations := pService.ObjectMeta.Annotations
	if svcAnnotations != nil &&
		len(svcAnnotations) > 0 &&
		svcAnnotations[lConsts.DEFAULT_K8S_SERVICE_ANNOTATION_PREFIX+"/owner-cluster-id"] == pCluster.ID {
		return true
	}

	return false
}

func ParsePoolAlgorithm(pOpt string) lPoolV2.CreateOptsAlgorithmOpt {
	opt := lStr.Replace(lStr.TrimSpace(lStr.ToUpper(pOpt)), "-", "_", -1)
	switch opt {
	case string(lPoolV2.CreateOptsAlgorithmOptSourceIP):
		return lPoolV2.CreateOptsAlgorithmOptRoundRobin
	case string(lPoolV2.CreateOptsAlgorithmOptLeastConn):
		return lPoolV2.CreateOptsAlgorithmOptLeastConn
	}
	return lPoolV2.CreateOptsAlgorithmOptRoundRobin
}

func ParsePoolProtocol(pPoolProtocol lCoreV1.Protocol) lPoolV2.CreateOptsProtocolOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(string(pPoolProtocol)))
	switch opt {
	case string(lPoolV2.CreateOptsProtocolOptProxy):
		return lPoolV2.CreateOptsProtocolOptProxy
	case string(lPoolV2.CreateOptsProtocolOptHTTP):
		return lPoolV2.CreateOptsProtocolOptHTTP
	case string(lPoolV2.CreateOptsProtocolOptUDP):
		return lPoolV2.CreateOptsProtocolOptUDP
	}
	return lPoolV2.CreateOptsProtocolOptTCP
}

func ParseMonitorProtocol(pPoolProtocol lCoreV1.Protocol) lPoolV2.CreateOptsHealthCheckProtocolOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(string(pPoolProtocol)))
	switch opt {
	case string(lPoolV2.CreateOptsProtocolOptUDP):
		return lPoolV2.CreateOptsHealthCheckProtocolOptPINGUDP
	}
	return lPoolV2.CreateOptsHealthCheckProtocolOptTCP
}

func ParseLoadBalancerScheme(pInternal bool) lLoadBalancerV2.CreateOptsSchemeOpt {
	if pInternal {
		return lLoadBalancerV2.CreateOptsSchemeOptInternal
	}
	return lLoadBalancerV2.CreateOptsSchemeOptInternet
}

func ParseListenerProtocol(pPort lCoreV1.ServicePort) lListenerV2.CreateOptsListenerProtocolOpt {
	opt := lStr.TrimSpace(lStr.ToUpper(string(pPort.Protocol)))
	switch opt {
	case string(lListenerV2.CreateOptsListenerProtocolOptUDP):
		return lListenerV2.CreateOptsListenerProtocolOptUDP
	}

	return lListenerV2.CreateOptsListenerProtocolOptTCP
}
