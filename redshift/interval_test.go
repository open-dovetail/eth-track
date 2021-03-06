package redshift

// Run all unit test: `go test -v`

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearch(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	index := blocks.search(15)
	assert.Equal(t, 1, index, "search result should be 1")
	//fmt.Println(index, blocks)
}

func TestSearchEmpty(t *testing.T) {
	blocks := NewBlockInterval(nil)
	index := blocks.search(15)
	assert.Equal(t, 0, index, "search result should be 0")
	//fmt.Println(index, blocks)
}

func TestSearchBelow(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	index := blocks.search(4)
	assert.Equal(t, 0, index, "search result should be 0")
	//fmt.Println(index, blocks)
}

func TestSearchAbove(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	index := blocks.search(60)
	assert.Equal(t, 3, index, "search result should be 3")
	//fmt.Println(index, blocks)
}

func TestAddBlockEmpty(t *testing.T) {
	blocks := NewBlockInterval(nil)
	blocks.AddBlock(15)
	assert.Equal(t, 1, blocks.Len(), "result should contain 1 interval")
	assert.Equal(t, uint64(15), blocks.next.Low, "updated interval low bound should be 15")
	//fmt.Println(blocks)
}

func TestAddBlockBelow(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(19)
	assert.Equal(t, 3, blocks.Len(), "result should contain 3 intervals")
	assert.Equal(t, uint64(19), blocks.next.Low, "updated interval low bound should be 19")
	//fmt.Println(blocks)
}

func TestAddBlockAbove(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(31)
	assert.Equal(t, 3, blocks.Len(), "result should contain 3 intervals")
	assert.Equal(t, uint64(31), blocks.next.High, "updated interval high bound should be 31")
	//fmt.Println(blocks)
}

func TestAddBlockMid(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(35)
	assert.Equal(t, 4, blocks.Len(), "result should contain 4 intervals")
	assert.Equal(t, uint64(35), blocks.working[2].High, "new interval high bound should be 35")
	//fmt.Println(blocks)
}

func TestAddBlockBelowAll(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(3)
	assert.Equal(t, 4, blocks.Len(), "result should contain 4 intervals")
	assert.Equal(t, uint64(3), blocks.working[0].High, "new interval high bound should be 3")
	//fmt.Println(blocks)
}

func TestAddBlockAboveAll(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(60)
	assert.Equal(t, 4, blocks.Len(), "result should contain 4 intervals")
	assert.Equal(t, uint64(60), blocks.working[3].High, "new interval high bound should be 60")
	//fmt.Println(blocks)
}

func TestAddBlockTop(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(56)
	//fmt.Println(blocks)
	assert.Equal(t, 3, blocks.Len(), "result should contain 3 intervals")
	assert.Equal(t, uint64(56), blocks.working[2].High, "top interval high bound should be 56")
}

func TestAddBlockBottom(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 10}, {50, 55}})
	blocks.AddBlock(4)
	assert.Equal(t, 3, blocks.Len(), "result should contain 3 intervals")
	assert.Equal(t, uint64(4), blocks.working[0].Low, "bottom interval low bound should be 4")
	//fmt.Println(blocks)
}

func TestAddBlockMerge(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 18}, {50, 55}})
	blocks.AddBlock(19)
	assert.Equal(t, 2, blocks.Len(), "merged result should contain 2 intervals")
	assert.Equal(t, uint64(30), blocks.next.High, "merged interval high bound should be 30")
	//fmt.Println(blocks)
}

func TestGetIntervalGaps(t *testing.T) {
	blocks := NewBlockInterval([]Interval{{20, 30}, {5, 18}, {50, 55}})
	gaps := blocks.GetIntervalGaps()
	assert.Equal(t, 2, len(gaps), "result should contain 2 gaps")
	assert.Equal(t, uint64(19), gaps[0].High, "high bound of the first gap should be 19")
	//fmt.Println(gaps)
}

func TestInitGaps(t *testing.T) {
	//secret, err := GetAWSSecret("dev/Redshift", "oocto", "us-west-2")
	//_, err = Connect(secret, "dev", 2)
	//require.NoError(t, err, "connect to redshift should not throw exception")
	bcache, err := GetBlockCache()
	gaps := bcache.GetIntervalGaps()
	fmt.Println("Processing gaps", gaps, "Range", bcache.scheduled)

	bi := NewBlockInterval([]Interval{})
	// query blocks and set blocks saved in the blocks table
	blocks, err := SelectBlocks(0, 0)
	require.NoError(t, err, "query blocks should not throw exception")
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
	gaps = bi.GetIntervalGaps()
	fmt.Println("Database gaps", gaps, "Range", bi.scheduled)
}
