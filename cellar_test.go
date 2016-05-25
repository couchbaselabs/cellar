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
	"fmt"
	"os"
	"testing"
)

func TestCellarCrudSimple(t *testing.T) {
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
}

func TestCellarCrudMultipleSegments(t *testing.T) {
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
}

func TestCellarCrudWithDeletesAndRecreates(t *testing.T) {
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

	// now we're going to delete a key
	err = c.Update(func(tx *Tx) error {
		err = tx.Delete([]byte("k0000000000000000"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// run checks
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkNoKey(t, tx, "k0000000000000000")
		checkCursor(t, tx, "k0000000000000001", "v0000000000000001", "k00000000000000c7", "v00000000000000c7", 199)
		return nil
	})

	// now bring that key back with different value
	err = c.Update(func(tx *Tx) error {
		err = tx.Put([]byte("k0000000000000000"), []byte("v000000000000000x"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// run checks
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000000", "v000000000000000x")
		checkCursor(t, tx, "k0000000000000000", "v000000000000000x", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})
}

func putKvPairs(tx *Tx, start, end int) error {
	for i := start; i < end; i++ {
		err := tx.Put([]byte(fmt.Sprintf("k%016x", i)), []byte(fmt.Sprintf("v%016x", i)))
		if err != nil {
			return err
		}
	}
	return nil
}

func checkNoKey(t *testing.T, tx *Tx, key string) {
	v := tx.Get([]byte(key))
	if v != nil {
		t.Errorf("expected key '%s' to be nil, got %s", key, string(v))
	}
}

func checkKey(t *testing.T, tx *Tx, key, expectedValue string) {
	v := tx.Get([]byte(key))
	if bytes.Compare(v, []byte(expectedValue)) != 0 {
		t.Errorf("expected key '%s' to have value '%s', got '%s'", key, expectedValue, string(v))
	}
}

func checkCursor(t *testing.T, tx *Tx, fk, fv, lk, lv string, ct int) {
	c := tx.Cursor()
	k, v := c.Seek([]byte{})
	firstk := k
	firstv := v
	lastk := k
	lastv := v
	count := 0
	for k != nil {
		lastk = k
		lastv = v
		count++
		k, v = c.Next()
	}
	if bytes.Compare(firstk, []byte(fk)) != 0 {
		t.Errorf("expected first key to be '%s', got '%s'", fk, firstk)
	}
	if bytes.Compare(firstv, []byte(fv)) != 0 {
		t.Errorf("expected first value to be '%s', got '%s'", fv, firstv)
	}
	if bytes.Compare(lastk, []byte(lk)) != 0 {
		t.Errorf("expected last key to be '%s', got '%s'", lk, lastk)
	}
	if bytes.Compare(lastv, []byte(lv)) != 0 {
		t.Errorf("expected last value to be '%s', got '%s'", lv, lastv)
	}
	if count != ct {
		t.Errorf("expected count %d, got %d", ct, count)
	}
}

func TestCellarCrudSimpleWithReopen(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}

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

	// close it
	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open it again
	c, err = Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// see if data is still there
	// let's see if we can retrieve it
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000000", "v0000000000000000")
		checkCursor(t, tx, "k0000000000000000", "v0000000000000000", "k0000000000000063", "v0000000000000063", 100)
		return nil
	})
}

func TestCellarCrudMultipleSegmentsWithReopen(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}

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

	// close it
	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open it again
	c, err = Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// check to see if everything is still here
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000064", "v0000000000000064")
		checkCursor(t, tx, "k0000000000000000", "v0000000000000000", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})
}

func TestCellarCrudWithDeletesAndRecreatesWithReopen(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}

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

	// now we're going to delete a key
	err = c.Update(func(tx *Tx) error {
		err = tx.Delete([]byte("k0000000000000000"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// run checks
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkNoKey(t, tx, "k0000000000000000")
		checkCursor(t, tx, "k0000000000000001", "v0000000000000001", "k00000000000000c7", "v00000000000000c7", 199)
		return nil
	})

	// now bring that key back with different value
	err = c.Update(func(tx *Tx) error {
		err = tx.Put([]byte("k0000000000000000"), []byte("v000000000000000x"))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// run checks
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000000", "v000000000000000x")
		checkCursor(t, tx, "k0000000000000000", "v000000000000000x", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})

	// close it
	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}

	// open it again
	c, err = Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// check to see if everything is still here
	c.View(func(tx *Tx) error {
		checkNoKey(t, tx, "doesnotexist")
		checkKey(t, tx, "k0000000000000000", "v000000000000000x")
		checkCursor(t, tx, "k0000000000000000", "v000000000000000x", "k00000000000000c7", "v00000000000000c7", 200)
		return nil
	})
}

func TestLongevity(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		err = c.Update(func(tx *Tx) error {
			err := putKvPairs(tx, i*100, (i+1)*100)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	err = c.Close()
	if err != nil {
		t.Fatal(err)
	}
}
