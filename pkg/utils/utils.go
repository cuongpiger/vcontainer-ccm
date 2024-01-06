package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/listener"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/loadbalancer/v2/pool"

	lCoreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	clientset "k8s.io/client-go/kubernetes"
)

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
