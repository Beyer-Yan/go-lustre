package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
import "C"

import (
	"fmt"
	"log"
)

type (
	Coordinator struct {
		hcp *C.struct_hsm_copytool_private
	}

	ActionItem struct {
		cdt       *Coordinator
		hai       C.struct_hsm_action_item
		hcap      *C.struct_hsm_copyaction_private
		halFlags  uint64
		archiveId uint
	}

	ActionItemHandle ActionItem
)

// CoordinatorConnection opens a connection to the coordinator.
func CoordinatorConnection(path RootDir) (*Coordinator, error) {
	var cdt = Coordinator{}

	_, err := C.llapi_hsm_copytool_register(&cdt.hcp, C.CString(string(path)), 0, nil, 0)
	if err != nil {
		return nil, err
	}
	return &cdt, nil
}

// Recv blocks and waits for new action items from the coordinator.
// Retuns a slice of ActionItems.
func (cdt *Coordinator) Recv() ([]ActionItem, error) {
	var hal *C.struct_hsm_action_list
	var hai *C.struct_hsm_action_item
	var msgsize C.int

	rc, err := C.llapi_hsm_copytool_recv(cdt.hcp, &hal, &msgsize)
	if rc < 0 || err != nil {
		return nil, err
	}
	defer C.llapi_hsm_action_list_free(&hal)

	items := make([]ActionItem, 0, int(hal.hal_count))
	hai = C.hai_first(hal)
	for i := 0; i < int(hal.hal_count); i++ {
		item := ActionItem{
			halFlags:  uint64(hal.hal_flags),
			archiveId: uint(hal.hal_archive_id),
			cdt:       cdt,
			hai:       *hai,
		}
		items = append(items, item)
		hai = C.hai_next(hai)
	}
	return items, nil
}

// Close terminates connection with coordinator.
func (cdt *Coordinator) Close() {
	C.llapi_hsm_copytool_unregister(&cdt.hcp)
	cdt.hcp = nil
}

// Begin prepares an ActionItem for processing.
//
// returns an ActionItemHandle. The End method must be called to complete
// this action.
func (ai *ActionItem) Begin(mdtIndex int, openFlags int, isError bool) (*ActionItemHandle, error) {
	rc, err := C.llapi_hsm_action_begin(&ai.hcap, ai.cdt.hcp, &ai.hai,
		C.int(mdtIndex),
		C.int(openFlags),
		C.bool(isError))
	if rc < 0 || err != nil {
		return nil, err
	}
	return (*ActionItemHandle)(ai), err
}

func (ai *ActionItem) String() string {
	return (*ActionItemHandle)(ai).String()
}

// ArchiveId returns the archive id associated with teh ActionItem.
func (ai *ActionItem) ArchiveId() uint {
	return ai.archiveId
}

// FailImmediately completes the ActinoItem with given error.
// The passed ActionItem is no longer valid when this function returns.
func (ai *ActionItem) FailImmediately(errval int) {
	aih, err := ai.Begin(0, 0, true)
	if err != nil {
		log.Println("begin failed: %s", ai.String())
		return
	}
	aih.End(0, 0, 0, errval)
}

func lengthStr(length uint64) string {
	if length == ^uint64(0) {
		return "EOF"
	}
	return fmt.Sprintf("%d", length)
}

func (ai *ActionItemHandle) String() string {
	return fmt.Sprintf("AI: %v %v %d,%v", ai.Action(), ai.Fid(), ai.Offset(), lengthStr(ai.Length()))
}

// Progress reports current progress of an action.
func (ai *ActionItemHandle) Progress(offset uint64, length uint64, totalLength uint64, flags int) error {
	extent := C.struct_hsm_extent{C.ulonglong(offset), C.ulonglong(length)}
	rc, err := C.llapi_hsm_action_progress(ai.hcap, &extent, C.ulonglong(totalLength), C.int(flags))
	if rc < 0 || err != nil {
		return err
	}
	return nil
}

// End completes an action with specified status.
// No more requests should be made on this action after calling this.
func (ai *ActionItemHandle) End(offset uint64, length uint64, flags int, errval int) error {
	extent := C.struct_hsm_extent{C.ulonglong(offset), C.ulonglong(length)}
	rc, err := C.llapi_hsm_action_end(&ai.hcap, &extent, C.int(flags), C.int(errval))
	if rc < 0 || err != nil {
		return err
	}
	return nil
}

type HsmAction uint32

const (
	NONE    = HsmAction(C.HSMA_NONE)
	ARCHIVE = HsmAction(C.HSMA_ARCHIVE)
	RESTORE = HsmAction(C.HSMA_RESTORE)
	REMOVE  = HsmAction(C.HSMA_REMOVE)
	CANCEL  = HsmAction(C.HSMA_CANCEL)
)

func (action HsmAction) String() string {
	return C.GoString(C.hsm_copytool_action2name(C.enum_hsm_copytool_action(action)))
	// i kinda prefer lowercase...
	// switch action {
	// case NONE:
	// 	return "noop"
	// case ARCHIVE:
	// 	return "archive"
	// case RESTORE:
	// 	return "restore"
	// case REMOVE:
	// 	return "remove"
	// case CANCEL:
	// 	return "cancel"
	// }
}

// Action returns name of the action.
func (ai *ActionItemHandle) Action() HsmAction {
	return HsmAction(ai.hai.hai_action)
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *ActionItemHandle) Fid() Fid {
	return Fid(ai.hai.hai_fid)
}

// DataFid returns the FID of the data file.
// This file should be used for all Lustre IO for archive and restore commands.
func (ai *ActionItemHandle) DataFid() (*Fid, error) {
	var fid Fid
	rc, err := C.llapi_hsm_action_get_dfid(ai.hcap, (*C.lustre_fid)(&fid))
	if rc < 0 || err != nil {
		return nil, err
	}
	return &fid, nil
}

// Fd returns the file descriptor of the DataFid.
// If used, this Fd must be closed prior to calling End.
func (ai *ActionItemHandle) Fd() (uintptr, error) {
	rc, err := C.llapi_hsm_action_get_fd(ai.hcap)
	if rc < 0 || err != nil {
		return 0, err
	}
	return uintptr(rc), nil
}

// Offset returns the offset for the action.
func (ai *ActionItemHandle) Offset() uint64 {
	return uint64(ai.hai.hai_extent.offset)
}

// Length returns the length of the action request.
func (ai *ActionItemHandle) Length() uint64 {
	return uint64(ai.hai.hai_extent.length)
}

// ArchiveId returns archive for this action.
// Duplicating this on the action allows actions to be
// self-contained.
func (ai *ActionItemHandle) ArchiveId() uint {
	return ai.archiveId
}