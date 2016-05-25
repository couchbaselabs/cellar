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
	"runtime"
	"testing"
)

var testOptionsNoAutoMerge = &Options{
	automaticMerge: false,
}

func TestMerge(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// let's lookup data before we've written anything
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		return nil
	})

	// let's write some data
	err = c.Update(func(tx *Tx) error {
		putKvPairs(tx, 0, 100)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// let's see if we can retrieve it
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000000", "v0000000000000000")
		checkCursor(t, tx, "k0000000000000000", "v0000000000000000", "k0000000000000063", "v0000000000000063", 100)
		return nil
	})

	// now write more data (second segment)
	err = c.Update(func(tx *Tx) error {
		putKvPairs(tx, 100, 200)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// let's see if we can retrieve it
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000064", "v0000000000000064")
		checkCursor(t, tx, "k0000000000000000", "v0000000000000000", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})

	// now force a merge
	numMergesBefore := c.Stats().mergesCompleted
	c.ForceMerge()
	numMerges := c.Stats().mergesCompleted
	for numMerges <= numMergesBefore {
		runtime.Gosched()
		numMerges = c.Stats().mergesCompleted
	}

	root := c.getRoot("TestMerge - test check count")
	// immediately decr the refs
	for _, segment := range root {
		segment.decrRef("test done with refs")
	}
	if len(root) != 1 {
		t.Fatalf("expected only 1 segment in root now, got %d", len(root))
	}

	// see if all data is still there
	// let's see if we can retrieve it
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000064", "v0000000000000064")
		checkCursor(t, tx, "k0000000000000000", "v0000000000000000", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})
}

// this test attempts to test some more advanced merging behavior
// specifically we create 5 segments
// segments 1 and 2 will be merged, dropping deletes
// segments 3 and 4 will be merged preserving delets
// setgment 5 will not be merged
//
// k00 - created in segment 1, never changed
// k01 - created in segment 1, mutated in segment 2
// k02 - created in segment 1, deleted in segment 2
// k03 - created in segment 1, mutated in segment 3
// k04 - created in segment 1, deleted in segment 3
// k05 - created in segment 1, mutated in segment 4
// k06 - created in segment 1, deleted in segment 4
// k07 - created in segment 1, mutated in segment 5
// k08 - created in segment 1, deleted in segment 5
//
// k10 - created in segment 2, never changed
// k11 - created in segment 2, mutated in segment 3
// k12 - created in segment 2, deleted in segment 3
// k13 - created in segment 2, mutated in segment 4
// k14 - created in segment 2, deleted in segment 4
// k15 - created in segment 2, mutated in segment 5
// k16 - created in segment 2, deleted in segment 5
//
// k20 - created in segment 3, never changed
// k21 - created in segment 3, mutated in segment 4
// k22 - created in segment 3, deleted in segment 4
// k23 - created in segment 3, mutated in segment 5
// k24 - created in segment 3, deleted in segment 5
//
// k30 - created in segment 4, never changed
// k31 - created in segment 4, mutated in segment 5
// k32 - created in segment 4, deleted in segment 5
//
// k40 - created in segment 5, never changed
func TestMergeAdvanced(t *testing.T) {

	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// segment 1
	err = c.Update(func(tx *Tx) error {
		err := tx.Put([]byte("k00"), []byte("v0s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k01"), []byte("v1s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k02"), []byte("v2s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k03"), []byte("v3s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k04"), []byte("v4s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k05"), []byte("v5s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k06"), []byte("v6s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k07"), []byte("v7s1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k08"), []byte("v8s1"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// check segment 1
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s1",
			"k02": "v2s1",
			"k03": "v3s1",
			"k04": "v4s1",
			"k05": "v5s1",
			"k06": "v6s1",
			"k07": "v7s1",
			"k08": "v8s1",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s1"},
			[]string{"k02", "v2s1"},
			[]string{"k03", "v3s1"},
			[]string{"k04", "v4s1"},
			[]string{"k05", "v5s1"},
			[]string{"k06", "v6s1"},
			[]string{"k07", "v7s1"},
			[]string{"k08", "v8s1"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// segment 2
	err = c.Update(func(tx *Tx) error {

		// k01 - created in segment 1, mutated in segment 2
		err := tx.Put([]byte("k01"), []byte("v1s2"))
		if err != nil {
			return err
		}
		// k02 - created in segment 1, deleted in segment 2
		err = tx.Delete([]byte("k02"))
		if err != nil {
			return err
		}

		err = tx.Put([]byte("k10"), []byte("v10s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k11"), []byte("v11s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k12"), []byte("v12s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k13"), []byte("v13s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k14"), []byte("v14s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k15"), []byte("v15s2"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k16"), []byte("v16s2"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// check after segment 2
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v3s1",
			"k04": "v4s1",
			"k05": "v5s1",
			"k06": "v6s1",
			"k07": "v7s1",
			"k08": "v8s1",

			"k10": "v10s2",
			"k11": "v11s2",
			"k12": "v12s2",
			"k13": "v13s2",
			"k14": "v14s2",
			"k15": "v15s2",
			"k16": "v16s2",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v3s1"},
			[]string{"k04", "v4s1"},
			[]string{"k05", "v5s1"},
			[]string{"k06", "v6s1"},
			[]string{"k07", "v7s1"},
			[]string{"k08", "v8s1"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s2"},
			[]string{"k12", "v12s2"},
			[]string{"k13", "v13s2"},
			[]string{"k14", "v14s2"},
			[]string{"k15", "v15s2"},
			[]string{"k16", "v16s2"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// segment 3
	err = c.Update(func(tx *Tx) error {

		// k03 - created in segment 1, mutated in segment 3
		err := tx.Put([]byte("k03"), []byte("v1s3"))
		if err != nil {
			return err
		}
		// k04 - created in segment 1, deleted in segment 3
		err = tx.Delete([]byte("k04"))
		if err != nil {
			return err
		}

		// k11 - created in segment 2, mutated in segment 3
		err = tx.Put([]byte("k11"), []byte("v11s3"))
		if err != nil {
			return err
		}
		// k12 - created in segment 2, deleted in segment 3
		err = tx.Delete([]byte("k12"))
		if err != nil {
			return err
		}

		err = tx.Put([]byte("k20"), []byte("v20s3"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k21"), []byte("v21s3"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k22"), []byte("v22s3"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k23"), []byte("v23s3"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k24"), []byte("v24s3"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// check after segment 3
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s1",
			"k06": "v6s1",
			"k07": "v7s1",
			"k08": "v8s1",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s2",
			"k14": "v14s2",
			"k15": "v15s2",
			"k16": "v16s2",

			"k20": "v20s3",
			"k21": "v21s3",
			"k22": "v22s3",
			"k23": "v23s3",
			"k24": "v24s3",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s1"},
			[]string{"k06", "v6s1"},
			[]string{"k07", "v7s1"},
			[]string{"k08", "v8s1"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s2"},
			[]string{"k14", "v14s2"},
			[]string{"k15", "v15s2"},
			[]string{"k16", "v16s2"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s3"},
			[]string{"k22", "v22s3"},
			[]string{"k23", "v23s3"},
			[]string{"k24", "v24s3"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// segment 4
	err = c.Update(func(tx *Tx) error {

		// k05 - created in segment 1, mutated in segment 4
		err := tx.Put([]byte("k05"), []byte("v5s4"))
		if err != nil {
			return err
		}
		// k06 - created in segment 1, deleted in segment 4
		err = tx.Delete([]byte("k06"))
		if err != nil {
			return err
		}

		// k13 - created in segment 2, mutated in segment 4
		err = tx.Put([]byte("k13"), []byte("v13s4"))
		if err != nil {
			return err
		}
		// k14 - created in segment 2, deleted in segment 4
		err = tx.Delete([]byte("k14"))
		if err != nil {
			return err
		}

		// k21 - created in segment 3, mutated in segment 4
		err = tx.Put([]byte("k21"), []byte("v21s4"))
		if err != nil {
			return err
		}
		// k22 - created in segment 3, deleted in segment 4
		err = tx.Delete([]byte("k22"))
		if err != nil {
			return err
		}

		err = tx.Put([]byte("k30"), []byte("v30s4"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k31"), []byte("v31s4"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("k32"), []byte("v32s4"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// check after segment 4
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s4",
			"k07": "v7s1",
			"k08": "v8s1",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s4",
			"k15": "v15s2",
			"k16": "v16s2",

			"k20": "v20s3",
			"k21": "v21s4",
			"k23": "v23s3",
			"k24": "v24s3",

			"k30": "v30s4",
			"k31": "v31s4",
			"k32": "v32s4",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s4"},
			[]string{"k07", "v7s1"},
			[]string{"k08", "v8s1"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s4"},
			[]string{"k15", "v15s2"},
			[]string{"k16", "v16s2"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s4"},
			[]string{"k23", "v23s3"},
			[]string{"k24", "v24s3"},

			[]string{"k30", "v30s4"},
			[]string{"k31", "v31s4"},
			[]string{"k32", "v32s4"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// segment 5
	err = c.Update(func(tx *Tx) error {

		// k07 - created in segment 1, mutated in segment 5
		err := tx.Put([]byte("k07"), []byte("v7s5"))
		if err != nil {
			return err
		}
		// k08 - created in segment 1, deleted in segment 5
		err = tx.Delete([]byte("k08"))
		if err != nil {
			return err
		}

		// k15 - created in segment 2, mutated in segment 5
		err = tx.Put([]byte("k15"), []byte("v15s5"))
		if err != nil {
			return err
		}
		// k16 - created in segment 2, deleted in segment 5
		err = tx.Delete([]byte("k16"))
		if err != nil {
			return err
		}

		// k23 - created in segment 3, mutated in segment 5
		err = tx.Put([]byte("k23"), []byte("v23s5"))
		if err != nil {
			return err
		}
		// k24 - created in segment 3, deleted in segment 5
		err = tx.Delete([]byte("k24"))
		if err != nil {
			return err
		}

		// k31 - created in segment 4, mutated in segment 5
		err = tx.Put([]byte("k31"), []byte("v31s5"))
		if err != nil {
			return err
		}
		// k32 - created in segment 4, deleted in segment 5
		err = tx.Delete([]byte("k32"))
		if err != nil {
			return err
		}

		err = tx.Put([]byte("k40"), []byte("v40s5"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// check after segment 5
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s4",
			"k07": "v7s5",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s4",
			"k15": "v15s5",

			"k20": "v20s3",
			"k21": "v21s4",
			"k23": "v23s5",

			"k30": "v30s4",
			"k31": "v31s5",

			"k40": "v40s5",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s4"},
			[]string{"k07", "v7s5"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s4"},
			[]string{"k15", "v15s5"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s4"},
			[]string{"k23", "v23s5"},

			[]string{"k30", "v30s4"},
			[]string{"k31", "v31s5"},

			[]string{"k40", "v40s5"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// now force a merge, and wait for 2 merges to finish
	numMergesBefore := c.Stats().mergesCompleted
	c.ForceMerge()
	numMerges := c.Stats().mergesCompleted
	for numMerges < numMergesBefore+2 {
		runtime.Gosched()
		numMerges = c.Stats().mergesCompleted
	}

	// check that there are 3 segments now
	// segment 5 (original unmerged)
	// segment 6 (1+2 deletes dropped)
	// segment 7 (3+4 deletes kept)
	// order 5,7,6
	root := c.getRoot("TestMergeAdvanced - test check count")
	// immediately decr the refs
	for _, segment := range root {
		segment.decrRef("test done with refs")
	}
	if len(root) != 3 {
		t.Fatalf("expected 3 segments in root now, got %d", len(root))
	}

	// see if all data is still there
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s4",
			"k07": "v7s5",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s4",
			"k15": "v15s5",

			"k20": "v20s3",
			"k21": "v21s4",
			"k23": "v23s5",

			"k30": "v30s4",
			"k31": "v31s5",

			"k40": "v40s5",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s4"},
			[]string{"k07", "v7s5"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s4"},
			[]string{"k15", "v15s5"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s4"},
			[]string{"k23", "v23s5"},

			[]string{"k30", "v30s4"},
			[]string{"k31", "v31s5"},

			[]string{"k40", "v40s5"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// now force another merge, and wait for 1 more merge to finish
	numMergesBefore = c.Stats().mergesCompleted
	c.ForceMerge()
	numMerges = c.Stats().mergesCompleted
	for numMerges < numMergesBefore+1 {
		runtime.Gosched()
		numMerges = c.Stats().mergesCompleted
	}

	// check that there are 2 segments now
	// segment 5 (original unmerged)
	// segment 8 (1+2+3+4 deletes dropped)
	// order 5,8
	root = c.getRoot("TestMergeAdvanced - test check count2")
	// immediately decr the refs
	for _, segment := range root {
		segment.decrRef("test done with refs")
	}
	if len(root) != 2 {
		t.Fatalf("expected 2 segments in root now, got %d", len(root))
	}

	// see if all data is still there
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s4",
			"k07": "v7s5",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s4",
			"k15": "v15s5",

			"k20": "v20s3",
			"k21": "v21s4",
			"k23": "v23s5",

			"k30": "v30s4",
			"k31": "v31s5",

			"k40": "v40s5",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s4"},
			[]string{"k07", "v7s5"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s4"},
			[]string{"k15", "v15s5"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s4"},
			[]string{"k23", "v23s5"},

			[]string{"k30", "v30s4"},
			[]string{"k31", "v31s5"},

			[]string{"k40", "v40s5"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// now force one final merge, and wait for 1 more merge to finish
	numMergesBefore = c.Stats().mergesCompleted
	c.ForceMerge()
	numMerges = c.Stats().mergesCompleted
	for numMerges < numMergesBefore+1 {
		runtime.Gosched()
		numMerges = c.Stats().mergesCompleted
	}

	// check that there is just 1 segment now
	// segment 9 (1+2+3+4 +5 deletes dropped)
	// order 9
	root = c.getRoot("TestMergeAdvanced - test check count3")
	// immediately decr the refs
	for _, segment := range root {
		segment.decrRef("test done with refs")
	}
	if len(root) != 1 {
		t.Fatalf("expected 1 segment in root now, got %d", len(root))
	}

	// see if all data is still there
	err = c.View(func(tx *Tx) error {
		checkWithGet(t, tx, map[string]string{
			"k00": "v0s1",
			"k01": "v1s2",
			"k03": "v1s3",
			"k05": "v5s4",
			"k07": "v7s5",

			"k10": "v10s2",
			"k11": "v11s3",
			"k13": "v13s4",
			"k15": "v15s5",

			"k20": "v20s3",
			"k21": "v21s4",
			"k23": "v23s5",

			"k30": "v30s4",
			"k31": "v31s5",

			"k40": "v40s5",
		})
		checkWithIterator(t, tx, [][]string{
			[]string{"k00", "v0s1"},
			[]string{"k01", "v1s2"},
			[]string{"k03", "v1s3"},
			[]string{"k05", "v5s4"},
			[]string{"k07", "v7s5"},

			[]string{"k10", "v10s2"},
			[]string{"k11", "v11s3"},
			[]string{"k13", "v13s4"},
			[]string{"k15", "v15s5"},

			[]string{"k20", "v20s3"},
			[]string{"k21", "v21s4"},
			[]string{"k23", "v23s5"},

			[]string{"k30", "v30s4"},
			[]string{"k31", "v31s5"},

			[]string{"k40", "v40s5"},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func checkWithGet(t *testing.T, tx *Tx, expected map[string]string) {
	for k, v := range expected {
		checkKey(t, tx, k, v)
	}
}

func checkWithIterator(t *testing.T, tx *Tx, pairs [][]string) {

	check := func(i int, k []byte, v []byte) {
		if i >= len(pairs) {
			t.Errorf("iterator has %d, pairs only %d", i, len(pairs))
			return
		}
		pair := pairs[i]
		if len(pair) != 2 {
			t.Fatalf("invalid k/v pair %d contains %d", i, len(pair))
		}
		if string(k) != pair[0] {
			t.Errorf("expected key: %s, got key %s", pair[0], string(k))
		}
		if string(v) != pair[1] {
			t.Errorf("expected val: %s, got val %s", pair[1], string(v))
		}
	}

	i := 0
	c := tx.Cursor()
	k, v := c.Seek([]byte{})
	for k != nil {
		check(i, k, v)
		k, v = c.Next()
		i++
	}

}
