package client

import (
	"github.com/cuongpiger/joat/utils"
	"github.com/vngcloud/vcontainer-sdk/client"
	"github.com/vngcloud/vcontainer-sdk/vcontainer"
	"k8s.io/klog/v2"
)

func LogCfg(pAuthOpts AuthOpts) {
	klog.V(5).Infof("Init client with config: %+v", pAuthOpts)
}

func NewVContainerClient(authOpts *AuthOpts) (*client.ProviderClient, error) {
	identityUrl := utils.NormalizeURL(authOpts.IdentityURL) + "v2"
	provider, _ := vcontainer.NewClient(identityUrl)
	err := vcontainer.Authenticate(provider, authOpts.ToOAuth2Options())

	return provider, err
}
