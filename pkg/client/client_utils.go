package client

import (
	"github.com/vngcloud/vcontainer-sdk/client"
	"github.com/vngcloud/vcontainer-sdk/vcontainer"
	"k8s.io/klog/v2"
)

func LogCfg(pAuthOpts AuthOpts) {
	klog.V(5).Infof("Init client with config: %+v", pAuthOpts)
}

func NewVContainerClient(authOpts *AuthOpts) (*client.ProviderClient, error) {
	provider, _ := vcontainer.NewClient(authOpts.IdentityURL)
	err := vcontainer.Authenticate(provider, authOpts.ToOAuth2Options())

	return provider, err
}
