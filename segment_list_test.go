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
	"bytes"
	"testing"
)

func TestSegmentListMarshalBinary(t *testing.T) {
	tests := []struct {
		segments segmentList
		encoding []byte
	}{
		{
			segments: segmentList{&segment{seq: 0}},
			encoding: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		},
		{
			segments: segmentList{&segment{seq: 0}, &segment{seq: 1}},
			encoding: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			segments: segmentList{&segment{seq: 27}, &segment{seq: 59}, &segment{seq: 3038}},
			encoding: []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0xde},
		},
	}

	for _, test := range tests {
		actual, err := test.segments.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(actual, test.encoding) != 0 {
			t.Errorf("expected %#v, got %#v", test.encoding, actual)
		}
	}

}
