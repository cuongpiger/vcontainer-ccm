/**
 * Author: Cuong. Duong Manh <cuongdm3@vng.com.vn>
 * Description: TODO
 */

package client

import (
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/identity/v2/extensions/oauth2"
	"github.com/vngcloud/vcontainer-sdk/vcontainer/services/identity/v2/tokens"
)

type (
	AuthOpts struct {
		IdentityURL       string `gcfg:"identity-url" mapstructure:"identity-url" name:"identity-url"`
		PortalURL         string `gcfg:"portal-url" mapstructure:"portal-url" name:"portal-url"`
		VServerGatewayURL string `gcfg:"vserver-gateway-url" mapstructure:"vserver-gateway-url" name:"vserver-gateway-url"`
		ClientID          string `gcfg:"client-id" mapstructure:"client-id" name:"client-id"`
		ClientSecret      string `gcfg:"client-secret" mapstructure:"client-secret" name:"client-secret"`
	}
)

func (s *AuthOpts) ToOAuth2Options() *oauth2.AuthOptions {
	return &oauth2.AuthOptions{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		AuthOptionsBuilder: &tokens.AuthOptions{
			IdentityEndpoint: s.IdentityURL,
		},
	}
}
