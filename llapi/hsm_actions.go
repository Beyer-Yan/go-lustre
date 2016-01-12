package llapi

/*
#cgo LDFLAGS: -llustreapi
#include <lustre/lustreapi.h>
#include <stdlib.h>

*/
import "C"

import (
	"fmt"
	"unsafe"
)

// HsmProgressState describes the HSM request progress state
type HsmProgressState uint32

const (
	HsmProgressWaiting = HsmProgressState(C.HPS_WAITING)
	HsmProgressRunning = HsmProgressState(C.HPS_RUNNING)
	HsmProgressDone    = HsmProgressState(C.HPS_DONE)
)

func (hps HsmProgressState) String() string {
	return C.GoString(C.hsm_progress_state2name(C.enum_hsm_progress_states(hps)))
}

// HsmCurrentAction describes the current in-progress action for a file
type HsmCurrentAction struct {
	Action   HsmUserAction
	State    HsmProgressState
	Location *HsmExtent
}

func (hca *HsmCurrentAction) String() string {
	return fmt.Sprintf("[%s:%s] (%dB)", hca.Action, hca.State, hca.Location.Length)
}

// GetHsmCurrentAction returns the current HSM action for the given file.
func GetHsmCurrentAction(filePath string) (*HsmCurrentAction, error) {
	hca := C.struct_hsm_current_action{}

	buf := C.CString(filePath)
	defer C.free(unsafe.Pointer(buf))
	rc, err := C.llapi_hsm_current_action(buf, &hca)
	if err != nil {
		return nil, err
	}
	if rc > 0 {
		return nil, fmt.Errorf("Got %d from llapi_hsm_current_action, expected 0", rc)
	}

	return &HsmCurrentAction{
		Action: HsmUserAction(hca.hca_action),
		State:  HsmProgressState(hca.hca_state),
		Location: &HsmExtent{
			Offset: uint64(hca.hca_location.offset),
			Length: uint64(hca.hca_location.length),
		},
	}, nil
}