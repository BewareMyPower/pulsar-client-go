// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"time"
)

type ackGroupingTracker interface {
	add(id MessageID)

	addCumulative(id MessageID)

	isDuplicate(id MessageID) bool

	flush()

	flushAndClean()

	close()
}

type ackFlushType int

const (
	flushOnly ackFlushType = iota
	flushAndClean
	flushAndClose
	flushToCache
)

func newAckGroupingTracker(options *AckGroupingOptions,
	ackIndividual func(id MessageID),
	ackCumulative func(id MessageID),
	ackList func(ids []MessageID)) ackGroupingTracker {
	if options == nil {
		options = &AckGroupingOptions{
			MaxSize: 1000,
			MaxTime: 100 * time.Millisecond,
		}
	}

	if options.MaxSize <= 1 {
		return &immediateAckGroupingTracker{
			ackIndividual: ackIndividual,
			ackCumulative: ackCumulative,
		}
	}

	c := &cachedAcks{
		maxNumAcks:                      int(options.MaxSize),
		pendingIndividualAcks:           make(map[int64]MessageID),
		pendingIndividualBatchIndexAcks: make(map[int64][]*trackingMessageID),
		lastCumulativeAck:               EarliestMessageID(),
		ackCumulative:                   ackCumulative,
		ackList:                         ackList,
	}

	timeout := time.NewTicker(time.Hour)
	if options.MaxTime > 0 {
		timeout = time.NewTicker(options.MaxTime)
	} else {
		timeout.Stop()
	}
	t := &timedAckGroupingTracker{
		ackIndividualCh:   make(chan MessageID),
		ackCumulativeCh:   make(chan MessageID),
		duplicateIDCh:     make(chan MessageID),
		duplicateResultCh: make(chan bool),
		flushCh:           make(chan ackFlushType),
		waitFlushCh:       make(chan bool),
		cachedAcksCh:      make(chan cachedAcks),
	}
	go func() {
		for {
			select {
			case id := <-t.ackIndividualCh:
				if c.addAndCheckIfFull(id) {
					c.flushIndividualAcks()
					if options.MaxTime > 0 {
						timeout.Reset(options.MaxTime)
					}
				}
			case id := <-t.ackCumulativeCh:
				c.tryUpdateLastCumulativeAck(id)
				if options.MaxTime <= 0 {
					c.flushCumulativeAck()
				}
			case id := <-t.duplicateIDCh:
				t.duplicateResultCh <- c.isDuplicate(id)
			case <-timeout.C:
				c.flush()
			case ackFlushType := <-t.flushCh:
				if ackFlushType == flushToCache {
					t.cachedAcksCh <- *c
					break
				}
				timeout.Stop()
				c.flush()
				if ackFlushType == flushAndClean {
					c.clean()
				}
				t.waitFlushCh <- true
				if ackFlushType == flushAndClose {
					return
				}
			}
		}
	}()
	return t
}

type immediateAckGroupingTracker struct {
	ackIndividual func(id MessageID)
	ackCumulative func(id MessageID)
}

func (i *immediateAckGroupingTracker) add(id MessageID) {
	i.ackIndividual(id)
}

func (i *immediateAckGroupingTracker) addCumulative(id MessageID) {
	i.ackCumulative(id)
}

func (i *immediateAckGroupingTracker) isDuplicate(id MessageID) bool {
	return false
}

func (i *immediateAckGroupingTracker) flush() {
}

func (i *immediateAckGroupingTracker) flushAndClean() {
}

func (i *immediateAckGroupingTracker) close() {
}

type cachedAcks struct {
	maxNumAcks int
	numAcks    int

	pendingIndividualAcks           map[int64]MessageID
	pendingIndividualBatchIndexAcks map[int64][]*trackingMessageID

	lastCumulativeAck     MessageID
	cumulativeAckRequired bool

	ackCumulative func(id MessageID)
	ackList       func(ids []MessageID)
}

func (t *cachedAcks) addAndCheckIfFull(id MessageID) bool {
	key := messageIDHash(id)
	if messageIDIsBatch(id) {
		ids, _ := t.pendingIndividualBatchIndexAcks[key]
		if trackingID, ok := id.(*trackingMessageID); ok {
			t.pendingIndividualBatchIndexAcks[key] = append(ids, trackingID)
		} else {
			var tracker *ackTracker
			if len(ids) > 0 {
				tracker = ids[0].tracker
			} else {
				tracker = newAckTracker(uint(id.BatchSize()))
			}
			tracker.ack(int(id.BatchIdx()))
			t.pendingIndividualBatchIndexAcks[key] = append(ids, &trackingMessageID{
				messageID: id.(*messageID),
				tracker:   tracker,
			})
		}
		t.numAcks++
	} else {
		if _, found := t.pendingIndividualAcks[key]; !found {
			t.pendingIndividualAcks[key] = id
			t.numAcks++
		}
		// There is no need to acknowledge these individual messages in the batch now
		if ids, found := t.pendingIndividualBatchIndexAcks[key]; found {
			delete(t.pendingIndividualBatchIndexAcks, key)
			t.numAcks -= len(ids)
		}
	}
	return t.numAcks >= t.maxNumAcks
}

func (t *cachedAcks) tryUpdateLastCumulativeAck(id MessageID) {
	if messageIDCompare(t.lastCumulativeAck, id) < 0 {
		t.lastCumulativeAck = id
		t.cumulativeAckRequired = true
	}
}

func (t *cachedAcks) isDuplicate(id MessageID) bool {
	if messageIDCompare(t.lastCumulativeAck, id) >= 0 {
		return true
	}
	key := messageIDHash(id)
	if _, found := t.pendingIndividualAcks[key]; found {
		return true
	}
	if id.BatchIdx() < 0 {
		return false
	}
	if ids, found := t.pendingIndividualBatchIndexAcks[key]; found {
		for _, cachedId := range ids {
			if id.BatchIdx() == cachedId.BatchIdx() {
				return true
			}
		}
	}
	return false
}

func (t *cachedAcks) flushIndividualAcks() {
	if t.numAcks == 0 {
		return
	}
	msgIDs := make([]MessageID, t.numAcks)
	i := 0
	for _, id := range t.pendingIndividualAcks {
		msgIDs[i] = id
		i++
	}
	t.pendingIndividualAcks = make(map[int64]MessageID)

	for _, ids := range t.pendingIndividualBatchIndexAcks {
		for _, id := range ids {
			msgIDs[i] = id
			i++
		}
	}
	t.pendingIndividualBatchIndexAcks = make(map[int64][]*trackingMessageID)

	t.ackList(msgIDs)
	t.numAcks = 0
}

func (t *cachedAcks) flushCumulativeAck() {
	if t.cumulativeAckRequired {
		t.ackCumulative(t.lastCumulativeAck)
		t.cumulativeAckRequired = false
	}
}

func (t *cachedAcks) flush() {
	t.flushIndividualAcks()
	t.flushCumulativeAck()
}

func (t *cachedAcks) clean() {
	t.pendingIndividualAcks = make(map[int64]MessageID)
	t.pendingIndividualBatchIndexAcks = make(map[int64][]*trackingMessageID)
	t.lastCumulativeAck = EarliestMessageID()
	t.cumulativeAckRequired = false
}

type timedAckGroupingTracker struct {
	ackIndividualCh   chan MessageID
	ackCumulativeCh   chan MessageID
	duplicateIDCh     chan MessageID
	duplicateResultCh chan bool
	flushCh           chan ackFlushType
	waitFlushCh       chan bool
	// Only used for test
	cachedAcksCh chan cachedAcks
}

func (t *timedAckGroupingTracker) add(id MessageID) {
	t.ackIndividualCh <- id
}

func (t *timedAckGroupingTracker) addCumulative(id MessageID) {
	t.ackCumulativeCh <- id
}

func (t *timedAckGroupingTracker) isDuplicate(id MessageID) bool {
	t.duplicateIDCh <- id
	return <-t.duplicateResultCh
}

func (t *timedAckGroupingTracker) flush() {
	t.flushCh <- flushOnly
	<-t.waitFlushCh
}

func (t *timedAckGroupingTracker) flushAndClean() {
	t.flushCh <- flushAndClean
	<-t.waitFlushCh
}

func (t *timedAckGroupingTracker) close() {
	t.flushCh <- flushAndClose
	<-t.waitFlushCh
}

// Only used for test
func (t *timedAckGroupingTracker) getCache() cachedAcks {
	t.flushCh <- flushToCache
	return <-t.cachedAcksCh
}
