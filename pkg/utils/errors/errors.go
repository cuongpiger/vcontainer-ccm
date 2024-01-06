package errors

import (
	"fmt"
	vconError "github.com/vngcloud/vcontainer-sdk/error"
)

// ********************************************** ErrNodeAddressNotFound **********************************************

func NewErrNodeAddressNotFound(pNodeName, pInfo string) vconError.IErrorBuilder {
	err := new(ErrNodeAddressNotFound)
	err.NodeName = pNodeName
	if pInfo != "" {
		err.Info = pInfo
	}
	return err
}

func IsErrNodeAddressNotFound(pErr error) bool {
	_, ok := pErr.(*ErrNodeAddressNotFound)
	return ok
}

func (s *ErrNodeAddressNotFound) Error() string {
	s.DefaultError = fmt.Sprintf("can not find address of node [NodeName: %s]", s.NodeName)
	return s.ChoseErrString()
}

type ErrNodeAddressNotFound struct {
	NodeName string
	vconError.BaseError
}
