/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package twister // import "github.com/solnx/twister/internal/twister"

import (
	"math/rand"
	"runtime"

	"github.com/mjolnir42/erebos"
	"github.com/solnx/legacy"
)

// Dispatch implements erebos.Dispatcher
func Dispatch(msg erebos.Transport) error {
	// send all messages from the same host to the same
	// handler to keep the ordering intact
	hostID, err := legacy.PeekHostID(msg.Value)
	if err != nil {
		return err
	}
	msg.HostID = hostID
	if msg.HostID == -1 {
		//Use random handler
		Handlers[rand.Int()%runtime.NumCPU()].InputChannel() <- &msg
	} else {
		Handlers[hostID%runtime.NumCPU()].InputChannel() <- &msg
	}

	return nil
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
