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

// *********************************************** ErrNoDefaultSecgroup ************************************************

func NewErrNoDefaultSecgroup(pProjectUUID, pInfo string) vconError.IErrorBuilder {
	err := new(ErrNoDefaultSecgroup)
	err.ProjectUUID = pProjectUUID
	if pInfo != "" {
		err.Info = pInfo
	}
	return err
}

func IsErrNoDefaultSecgroup(pErr error) bool {
	_, ok := pErr.(*ErrNoDefaultSecgroup)
	return ok
}

func (s *ErrNoDefaultSecgroup) Error() string {
	s.DefaultError = fmt.Sprintf("the project %s has no default secgroup", s.ProjectUUID)
	return s.ChoseErrString()
}

type ErrNoDefaultSecgroup struct {
	ProjectUUID string
	vconError.BaseError
}

// ************************************************ ErrConflictService *************************************************

func NewErrConflictService(pPort int, pProtocol, pInfo string) vconError.IErrorBuilder {
	err := new(ErrConflictService)
	err.Port = pPort
	err.Protocol = pProtocol
	if pInfo != "" {
		err.Info = pInfo
	}
	return err
}

func IsErrConflictService(pErr error) bool {
	_, ok := pErr.(*ErrNoDefaultSecgroup)
	return ok
}

func (s *ErrConflictService) Error() string {
	s.DefaultError = fmt.Sprintf("this port [%d] and [%s] is already used by other service", s.Port, s.Protocol)
	return s.ChoseErrString()
}

type ErrConflictService struct {
	Port     int
	Protocol string
	vconError.BaseError
}
