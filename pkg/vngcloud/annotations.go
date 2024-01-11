package vngcloud

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"strconv"
)

const (
	ServiceAnnotationLoadBalancerID            = "vcontainer.vngcloud.vn/load-balancer-id"        // set via annotation
	ServiceAnnotationLoadBalancerName          = "vcontainer.vngcloud.vn/load-balancer-name"      // only set via the annotation
	ServiceAnnotationPackageID                 = "vcontainer.vngcloud.vn/package-id"              // both annotation and cloud-config
	ServiceAnnotationEnableSecgroupDefault     = "vcontainer.vngcloud.vn/enable-secgroup-default" // set via annotation
	ServiceAnnotationIdleTimeoutClient         = "vcontainer.vngcloud.vn/idle-timeout-client"     // both annotation and cloud-config
	ServiceAnnotationIdleTimeoutMember         = "vcontainer.vngcloud.vn/idle-timeout-member"     // both annotation and cloud-config
	ServiceAnnotationIdleTimeoutConnection     = "vcontainer.vngcloud.vn/idle-timeout-connection" // both annotation and cloud-config
	ServiceAnnotationListenerAllowedCIDRs      = "vcontainer.vngcloud.vn/listener-allowed-cidrs"  // both annotation and cloud-config
	ServiceAnnotationPoolAlgorithm             = "vcontainer.vngcloud.vn/pool-algorithm"          // both annotation and cloud-config
	ServiceAnnotationPoolProtocol              = "vcontainer.vngcloud.vn/pool-protocol"           // both annotation and cloud-config
	ServiceAnnotationMonitorProtocol           = "vcontainer.vngcloud.vn/monitor-protocol"        // both annotation and cloud-config
	ServiceAnnotationHealthyThreshold          = "vcontainer.vngcloud.vn/monitor-healthy-threshold"
	ServiceAnnotationMonitorUnhealthyThreshold = "vcontainer.vngcloud.vn/monitor-unhealthy-threshold"
	ServiceAnnotationMonitorTimeout            = "vcontainer.vngcloud.vn/monitor-timeout"
	ServiceAnnotationMonitorInterval           = "vcontainer.vngcloud.vn/monitor-interval"
	ServiceAnnotationMonitorHTTPMethod         = "vcontainer.vngcloud.vn/monitor-http-method"
	ServiceAnnotationMonitorHTTPPath           = "vcontainer.vngcloud.vn/monitor-http-path"
	ServiceAnnotationMonitorHTTPSuccessCode    = "vcontainer.vngcloud.vn/monitor-http-success-code"
	ServiceAnnotationMonitorHTTPVersion        = "vcontainer.vngcloud.vn/monitor-http-version"
	ServiceAnnotationMonitorHTTPDomainName     = "vcontainer.vngcloud.vn/monitor-http-domain-name"
	ServiceAnnotationLoadBalancerInternal      = "service.beta.kubernetes.io/vngcloud-internal-load-balancer"
)

// getStringFromServiceAnnotation searches a given v1.Service for a specific annotationKey and either returns the annotation's value or a specified defaultSetting
func getStringFromServiceAnnotation(service *corev1.Service, annotationKey string, defaultSetting string) string {
	klog.V(4).Infof("getStringFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		//if there is an annotation for this setting, set the "setting" var to it
		// annotationValue can be empty, it is working as designed
		// it makes possible for instance provisioning loadbalancer without floatingip
		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, annotationValue)
		return annotationValue
	}

	//if there is no annotation, set "settings" var to the value from cloud config
	if defaultSetting != "" {
		klog.V(4).Infof("Could not find a Service Annotation; falling back on cloud-config setting: %v = %v", annotationKey, defaultSetting)
	}

	return defaultSetting
}

// getIntFromServiceAnnotation searches a given v1.Service for a specific annotationKey and either returns the annotation's integer value or a specified defaultSetting
func getIntFromServiceAnnotation(service *corev1.Service, annotationKey string, defaultSetting int) int {
	klog.V(4).Infof("getIntFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		returnValue, err := strconv.Atoi(annotationValue)
		if err != nil {
			klog.Warningf("Could not parse int value from %q, failing back to default %s = %v, %v", annotationValue, annotationKey, defaultSetting, err)
			return defaultSetting
		}

		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, annotationValue)
		return returnValue
	}
	klog.V(4).Infof("Could not find a Service Annotation; falling back to default setting: %v = %v", annotationKey, defaultSetting)
	return defaultSetting
}

// getBoolFromServiceAnnotation searches a given v1.Service for a specific annotationKey and either returns the annotation's boolean value or a specified defaultSetting
// If the annotation is not found or is not a valid boolean ("true" or "false"), it falls back to the defaultSetting and logs a message accordingly.
func getBoolFromServiceAnnotation(service *corev1.Service, annotationKey string, defaultSetting bool) bool {
	klog.V(4).Infof("getBoolFromServiceAnnotation(%s/%s, %v, %v)", service.Namespace, service.Name, annotationKey, defaultSetting)
	if annotationValue, ok := service.Annotations[annotationKey]; ok {
		returnValue := false
		switch annotationValue {
		case "true":
			returnValue = true
		case "false":
			returnValue = false
		default:
			klog.Infof("Found a non-boolean Service Annotation: %v = %v (falling back to default setting: %v)", annotationKey, annotationValue, defaultSetting)
			return defaultSetting
		}

		klog.V(4).Infof("Found a Service Annotation: %v = %v", annotationKey, returnValue)
		return returnValue
	}
	klog.V(4).Infof("Could not find a Service Annotation; falling back to default setting: %v = %v", annotationKey, defaultSetting)
	return defaultSetting
}
