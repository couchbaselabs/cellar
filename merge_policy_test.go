//  Copyright (c) 2016 Couchbase, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cellar

import (
	"reflect"
	"testing"
)

func TestMergePolicy(t *testing.T) {
	tests := []struct {
		input  segmentList
		output []*Merge
	}{
		// no segments = no merges
		{
			input:  segmentList{},
			output: nil,
		},
		// 1 segment, nothing to merge
		{
			input: segmentList{
				&segment{
					seq: 1,
				},
			},
			output: nil,
		},
		// 2 segment, 1 merge
		{
			input: segmentList{
				&segment{
					seq: 2,
				},
				&segment{
					seq: 1,
				},
			},
			output: []*Merge{
				&Merge{
					sources: segmentList{
						&segment{
							seq: 2,
						},
						&segment{
							seq: 1,
						},
					},
					dropDeletes: true,
				},
			},
		},
		// 3 segment, without 2 consecutive
		{
			input: segmentList{
				&segment{
					seq: 3,
				},
				&segment{
					seq:             2,
					mergeInProgress: 4,
				},
				&segment{
					seq: 1,
				},
			},
			output: nil,
		},
		// 3 segments, oldest 2 get merged
		{
			input: segmentList{
				&segment{
					seq: 3,
				},
				&segment{
					seq: 2,
				},
				&segment{
					seq: 1,
				},
			},
			output: []*Merge{
				&Merge{
					sources: segmentList{
						&segment{
							seq: 2,
						},
						&segment{
							seq: 1,
						},
					},
					dropDeletes: true,
				},
			},
		},
		// 4 segments, 2 merges
		{
			input: segmentList{
				&segment{
					seq: 4,
				},
				&segment{
					seq: 3,
				},
				&segment{
					seq: 2,
				},
				&segment{
					seq: 1,
				},
			},
			output: []*Merge{
				&Merge{
					sources: segmentList{
						&segment{
							seq: 2,
						},
						&segment{
							seq: 1,
						},
					},
					dropDeletes: true,
				},
				&Merge{
					sources: segmentList{
						&segment{
							seq: 4,
						},
						&segment{
							seq: 3,
						},
					},
					dropDeletes: false,
				},
			},
		},
	}

	m := &SimpleMergePolicy{}
	for _, test := range tests {
		actual := m.Merges(nil, test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %v, got %v", test.output, actual)
		}
	}

}
