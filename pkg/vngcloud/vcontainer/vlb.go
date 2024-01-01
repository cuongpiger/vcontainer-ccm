package vcontainer

import (
	"context"
	"github.com/vngcloud/vcontainer-sdk/client"

	corev1 "k8s.io/api/core/v1"
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
	vLBSC *client.ServiceClient
}

func (s *vLB) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (*corev1.LoadBalancerStatus, bool, error) {
	return nil, false, nil
}

func (s *vLB) GetLoadBalancerName(_ context.Context, clusterName string, service *corev1.Service) string {
	return ""
}

func (s *vLB) EnsureLoadBalancer(ctx context.Context, clusterName string, apiService *corev1.Service, nodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {
	return nil, nil
}

func (s *vLB) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	return nil
}

func (s *vLB) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	return nil
}
