package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	lCoreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"
	lSecRuleV2 "github.com/vngcloud/vcontainer-sdk/vcontainer/services/network/v2/extensions/secgroup_rule"

	"github.com/cuongpiger/vcontainer-ccm/pkg/consts"
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

// Sprintf255 formats according to a format specifier and returns the resulting string with a maximum length of 255 characters.
func Sprintf50(format string, args ...interface{}) string {
	return CutString50(fmt.Sprintf(format, args...))
}

// CutString255 makes sure the string length doesn't exceed 255, which is usually the maximum string length in OpenStack.
func CutString50(original string) string {
	ret := original
	if len(original) > 50 {
		ret = original[:50]
	}
	return ret
}

func GetVLBProtocolOpt(pPort lCoreV1.ServicePort) pool.CreateOptsProtocolOpt {
	// Only support TCP at this version
	return pool.CreateOptsProtocolOptTCP
}

func GetListenerProtocolOpt(pPort lCoreV1.ServicePort) listener.CreateOptsListenerProtocolOpt {
	// Only support TCP at this version
	return listener.CreateOptsListenerProtocolOptTCP
}

func GetSecgroupRuleProtocolOpt(pPort lCoreV1.ServicePort) lSecRuleV2.CreateOptsProtocolOpt {
	// Only support TCP at this version
	return lSecRuleV2.CreateOptsProtocolOptTCP
}

func GetSecgroupRuleEthernetType(pIpFam lCoreV1.IPFamily) lSecRuleV2.CreateOptsEtherTypeOpt {
	if pIpFam == lCoreV1.IPv6Protocol {
		return lSecRuleV2.CreateOptsEtherTypeOptIPv6
	}

	return lSecRuleV2.CreateOptsEtherTypeOptIPv4
}

func GenPoolName(pClusterID string, pService *lCoreV1.Service, pProtocol string) string {
	lbName := GenLoadBalancerName(pClusterID, pService)[len(consts.DEFAULT_LB_PREFIX_NAME)+1:]
	delta := consts.DEFAULT_PORTAL_NAME_LENGTH - len(lbName) - 1
	if delta >= len(pProtocol) {
		return fmt.Sprintf("%s-%s", lbName, pProtocol)
	}

	delta = consts.DEFAULT_PORTAL_NAME_LENGTH - len(pProtocol) - 1
	return fmt.Sprintf("%s-%s", lbName[:delta], pProtocol)
}

func GenListenerName(pClusterID string, pService *lCoreV1.Service, pProtocol string, pPort int) string {
	port := fmt.Sprintf("%d", pPort)
	delta := consts.DEFAULT_PORTAL_NAME_LENGTH - len(port) - 2
	if delta >= len(pProtocol) {
		return fmt.Sprintf("%s-%s", pProtocol, port)
	}

	delta = consts.DEFAULT_PORTAL_NAME_LENGTH - len(pProtocol) - len(port) - 2
	return fmt.Sprintf("%s-%s", pProtocol, port)
}

func GenSecgroupDescription(pClusterID string, pService *lCoreV1.Service, pLbID string) string {
	lbName := GenLoadBalancerDescription(pClusterID, pService)
	delta := consts.DEFAULT_PORTAL_DESCRIPTION_LENGTH - len(lbName) - 1
	if delta >= len(pLbID) {
		return fmt.Sprintf("%s-%s", lbName, pLbID)
	}

	delta = consts.DEFAULT_PORTAL_DESCRIPTION_LENGTH - len(pLbID) - 1
	return fmt.Sprintf("%s-%s", lbName[:delta], pLbID)
}

func GenSecgroupName(pClusterID string, pService *lCoreV1.Service, pProtocol string, pPort int) string {
	lbName := GenLoadBalancerName(pClusterID, pService)[len(consts.DEFAULT_LB_PREFIX_NAME)+1:]
	port := fmt.Sprintf("%d", pPort)
	delta := consts.DEFAULT_PORTAL_NAME_LENGTH - len(lbName) - len(port) - 2
	if delta >= len(pProtocol) {
		return fmt.Sprintf("%s-%s-%s", lbName, pProtocol, port)
	}

	delta = consts.DEFAULT_PORTAL_NAME_LENGTH - len(pProtocol) - len(port) - 2
	return fmt.Sprintf("%s-%s-%s", lbName[:delta], pProtocol, port)
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
		"%s-%s-%s_%s", consts.DEFAULT_LB_PREFIX_NAME, pClusterID[8:19], pService.Name, pService.Namespace)
	return lbName[:MinInt(len(lbName), consts.DEFAULT_PORTAL_NAME_LENGTH)]
}

func GenLoadBalancerDescription(pClusterID string, pService *lCoreV1.Service) string {
	lbName := fmt.Sprintf(
		"%s-%s-%s_%s", consts.DEFAULT_LB_PREFIX_NAME, pClusterID[8:19], pService.Name, pService.Namespace)
	return lbName[:MinInt(len(lbName), consts.DEFAULT_PORTAL_DESCRIPTION_LENGTH)]
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
