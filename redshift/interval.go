package redshift

import (
	"sort"
	"sync"

	"github.com/open-dovetail/eth-track/common"
	"github.com/pkg/errors"
)

type Interval struct {
	Low  uint64
	High uint64
}

// continuous block intervals already stored in database.
type BlockInterval struct {
	sync.Mutex
	prev      Interval   // interval stored as confirmed consequtive blocks in progress table
	next      Interval   // interval of confirmed consequtive blocks to be updated in database
	working   []Interval // block intervals processed and confirmed at runtime
	scheduled Interval   // scheduled block min and max at runtime
}

// singleton block progress cache
var blockCache *BlockInterval

func GetBlockCache() (*BlockInterval, error) {
	if blockCache != nil {
		return blockCache, nil
	}

	// construct working interval from database
	var err error
	blockCache, err = queryBlockInterval()
	return blockCache, err
}

func NewBlockInterval(blocks []Interval) *BlockInterval {
	bi := &BlockInterval{working: []Interval{}}
	// ignore empty blocks
	if len(blocks) == 0 || (len(blocks) == 1 && blocks[0].High == 0 && blocks[0].Low == 0) {
		return bi
	}

	// set prev to the largest interval
	maxLen := 0
	index := -1
	for i, b := range blocks {
		if b.High > 0 && b.Low > 0 {
			bi.working = append(bi.working, b)
		}
		m := int(b.High - b.Low + 1)
		if m > maxLen {
			maxLen = m
			index = i
		}
	}
	if index >= 0 {
		bi.prev = blocks[index]
		bi.next = blocks[index]
	}

	// sort working intervals
	sort.Sort(bi)
	return bi
}

// query database to construct BlockInterval
func queryBlockInterval() (*BlockInterval, error) {
	// query progress table to get stored blocks
	progress, err := QueryProgress(common.AddTransaction)
	if err != nil {
		return nil, err
	}
	if progress == nil {
		return nil, errors.Errorf("progress db table not initialized for pid %d", common.AddTransaction)
	}
	bi := NewBlockInterval([]Interval{{progress.LowBlock, progress.HiBlock}})

	// query blocks and set blocks saved in the blocks table
	blocks, err := SelectBlocks(int64(progress.HiBlock), int64(progress.LowBlock))
	if err != nil {
		return nil, err
	}
	for _, v := range blocks {
		bi.AddBlock(uint64(*v))
	}
	if len(bi.working) > 0 {
		// initialize interval after gaps are filled in database
		bi.scheduled = Interval{
			Low:  bi.working[0].Low,
			High: bi.working[len(bi.working)-1].High,
		}
	}
	return bi, nil
}

// save progress in database if interval changed
func (s *BlockInterval) SaveNextInterval() error {
	// make updates thread-safe
	s.Lock()
	defer s.Unlock()

	if s.next.High == s.prev.High && s.next.Low == s.prev.Low {
		// interval not changed, so do nothing
		return nil
	}
	progress := &common.Progress{
		ProcessID: common.AddTransaction,
		HiBlock:   s.next.High,
		LowBlock:  s.next.Low,
	}
	if err := UpdateProgress(progress); err != nil {
		return err
	}
	s.prev = s.next
	return nil
}

// implement Sort interface for s.working
func (s *BlockInterval) Len() int {
	return len(s.working)
}

func (s *BlockInterval) Swap(i, j int) {
	s.working[i], s.working[j] = s.working[j], s.working[i]
}

func (s *BlockInterval) Less(i, j int) bool {
	return s.working[i].Low < s.working[j].Low
}

// return the lowest index of the interval with higher block numbers
func (s *BlockInterval) search(block uint64) int {
	return sort.Search(len(s.working), func(i int) bool { return s.working[i].Low >= block })
}

// update BlockInterval by adding a new block
func (s *BlockInterval) AddBlock(block uint64) {
	// make updates thread-safe
	s.Lock()
	defer s.Unlock()

	i := s.search(block)
	if i == s.Len() && i > 0 && s.working[i-1].High == block-1 {
		// extends the last interval
		s.working[i-1].High = block
		if s.next.Low >= s.working[i-1].Low && s.working[i-1].High >= s.next.High {
			s.next = s.working[i-1]
		}
	} else if i >= s.Len() {
		// append new interval
		s.working = append(s.working, Interval{block, block})
		if s.Len() == 1 {
			s.next = Interval{block, block}
		}
	} else if s.working[i].Low == block {
		// block is already counted, do nothing
	} else if s.working[i].Low == block+1 {
		// extend the working interval i
		s.working[i].Low = block
		if i > 0 && s.working[i-1].High == block-1 {
			// merge 2 intervals
			s.working[i].Low = s.working[i-1].Low
		}
		if s.next.Low >= s.working[i].Low && s.working[i].High >= s.next.High {
			s.next = s.working[i]
		}
		if i > 0 && s.working[i-1].Low >= s.working[i].Low {
			// remove interval i-1
			copy(s.working[i-1:], s.working[i:])
			s.working = s.working[:len(s.working)-1]
		}
	} else if i > 0 && s.working[i-1].High == block-1 {
		// extend the working interval i-1
		s.working[i-1].High = block
		if s.next.Low >= s.working[i-1].Low && s.working[i-1].High >= s.next.High {
			s.next = s.working[i-1]
		}
	} else {
		// add interval before i
		s.working = append(s.working, Interval{block, block})
		if i < len(s.working)-1 {
			copy(s.working[i+1:], s.working[i:])
			s.working[i] = Interval{block, block}
		}
	}
}

// return interval gaps between current working intervals
func (s *BlockInterval) GetIntervalGaps() []Interval {
	var result []Interval
	bound := uint64(0)
	for _, w := range s.working {
		if bound > 0 {
			result = append(result, Interval{bound, w.Low - 1})
		}
		bound = w.High + 1
	}
	return result
}

// return min and max blocks already scheduled at runtime
func (s *BlockInterval) GetScheduledBlocks() Interval {
	return s.scheduled
}

// update scheduled interval at runtime
func (s *BlockInterval) SetScheduledBlocks(schedule Interval) {
	s.Lock()
	defer s.Unlock()

	s.scheduled = schedule
}
