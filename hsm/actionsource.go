// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm

import (
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/intel-hpdd/logging/alert"
	"github.com/intel-hpdd/logging/debug"
	"github.com/intel-hpdd/go-lustre/fs"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
)

// ActionSource is a source of HSM actions
type ActionSource interface {
	// Actions is a channel for HSM actions. Mutiple listeners can use this
	// channel.
	// The channel will be closed when the ActionSource is shutdown.
	Actions() <-chan ActionRequest

	// Start signals the action source to begin sending actions
	Start(context.Context) error
}

type coordinatorSource struct {
	fsRoot  fs.RootDir
	actions <-chan ActionRequest
	maxListenQueueDepth int
}

// NewActionSource initializes an ActionSource for the filesystem in root.
func NewActionSource(root fs.RootDir, queueDepth int) ActionSource {
	return &coordinatorSource{fsRoot: root, maxListenQueueDepth: queueDepth}
}

// Start signals the source to begin sending actions
func (src *coordinatorSource) Start(ctx context.Context) error {
	// This pipe is used by Stop() to send the terminate signal to actionListener.
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}

	if err := src.actionListener(r); err != nil {
		return err
	}

	// Wait for the context to be canceled, then tell the other
	// side that we're closing up shop...
	go func() {
		<-ctx.Done()
		// Aribitrary data to wake up listener
		w.Write([]byte("stop"))
		w.Close()
	}()

	return nil
}

func (src *coordinatorSource) Actions() <-chan ActionRequest {
	return src.actions
}

func getFd(f *os.File) int {
	return int(f.Fd())
}

func (src *coordinatorSource) actionListener(stopFile *os.File) error {
	var err error
	cdc, err := NewCoordinatorClient(src.fsRoot, true)
	if err != nil {
		return fmt.Errorf("%s: %s", src.fsRoot, err)
	}

	ch := make(chan ActionRequest, src.maxListenQueueDepth)

	go func() {
		var events = make([]unix.EpollEvent, 2)
		var ev unix.EpollEvent
		epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			alert.Fatal(err)
		}
		ev.Fd = int32(getFd(stopFile))
		ev.Events = unix.EPOLLIN | unix.EPOLLET
		err = unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, getFd(stopFile), &ev)
		if err != nil {
			alert.Abort(errors.Wrap(err, "epollctl stopfile failed"))
		}

		ev.Fd = int32(cdc.GetFd())
		ev.Events = unix.EPOLLIN | unix.EPOLLET
		err = unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, cdc.GetFd(), &ev)
		if err != nil {
			alert.Abort(errors.Wrap(err, "epollctl coordinator fd failed"))
		}

		defer func() {
			cdc.Close()
			stopFile.Close()
			unix.Close(epfd)
			close(ch)
		}()

		for {
			var actions []*actionItem
			nfds, err := unix.EpollWait(epfd, events, -1)
			if err != nil {
				if err == unix.EINTR {
					continue
				}
				alert.Fatal(err)
			}

			for n := 0; n < nfds; n++ {
				ev := events[n]
				switch int(ev.Fd) {
				case getFd(stopFile):
					buf := make([]byte, 32)
					stopFile.Read(buf)
					return
				case cdc.GetFd():
					for {
						actions, err = cdc.recv()
						if err == unix.EAGAIN {
							break
						}
						if err != nil {
							debug.Print(err)
							return
						}
						for _, ai := range actions {
							ch <- ai
						}
					}
				}

			}

		}
	}()

	src.actions = ch
	return nil
}
