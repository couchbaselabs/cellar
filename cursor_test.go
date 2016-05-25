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
	"os"
	"testing"
)

func TestCellarCursorSimple(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// let's write some data
	err = c.Update(func(tx *Tx) error {
		putKvPairs(tx, 0, 100)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// lets try a cursor
	err = c.View(func(tx *Tx) error {
		c := tx.Cursor()

		k, _ := c.Next()
		if k != nil {
			t.Errorf("next on cursor before positioning expects nil, got key %s", string(k))
		}

		// seek to beginning
		k, _ = c.Seek([]byte(""))
		if string(k) != "k0000000000000000" {
			t.Errorf("expected to see key 'k0000000000000000' got %s", string(k))
		}

		// seek further ahead
		k, _ = c.Seek([]byte("k0000000000000004"))
		if string(k) != "k0000000000000004" {
			t.Errorf("expected to see key 'k0000000000000004' got %s", string(k))
		}

		// next after seq
		k, _ = c.Next()
		if string(k) != "k0000000000000005" {
			t.Errorf("expected to see key 'k0000000000000005' got %s", string(k))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

}
