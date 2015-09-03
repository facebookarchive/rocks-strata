package strata

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestMergeFileSets(t *testing.T) {
	mergedSet := mergeFileSets(
		[]File{{Name: "a"}, {Name: "b"}},
		[]File{{Name: "a"}, {Name: "c"}})
	ensure.SameElements(t, []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}, mergedSet)
	mergedSet = mergeFileSets(
		[]File{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		[]File{})
	ensure.SameElements(t, []File{{Name: "a"}, {Name: "b"}, {Name: "c"}}, mergedSet)
}

func TestSubtractFileSets(t *testing.T) {
	newSet := subtractFileSets(
		[]File{{Name: "a"}, {Name: "b"}, {Name: "c"}},
		[]File{{Name: "a"}, {Name: "c"}})
	ensure.SameElements(t, []File{{Name: "b"}}, newSet)
}
