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
	"fmt"
	"os"
	"testing"
)

func TestTxInvalidState(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.View(func(tx *Tx) error {
		err := tx.Put([]byte("k"), []byte("v"))
		if err != ErrTxNotWritable {
			t.Errorf("expected ErrTxNotWritable, got %v", err)
		}
		err = tx.Delete([]byte("k"))
		if err != ErrTxNotWritable {
			t.Errorf("expected ErrTxNotWritable, got %v", err)
		}
		return nil
	})

	c.Update(func(tx *Tx) error {
		err := tx.Rollback()
		if err != ErrTxIsManaged {
			t.Errorf("expected ErrTxIsManaged, got %v", err)
		}
		err = tx.Commit()
		if err != ErrTxIsManaged {
			t.Errorf("expected ErrTxIsManaged, got %v", err)
		}
		return nil
	})

	// create a read-only tx
	tx, err := c.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	// try to commit it anyway
	err = tx.Commit()
	if err != ErrTxNotWritable {
		t.Errorf("expected ErrTxNotWritable, got %v", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	tx, err = c.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Put([]byte("k"), []byte("v"))
	if err != ErrTxClosed {
		t.Errorf("expected ErrTxClosed, got %v", err)
	}
	err = tx.Delete([]byte("k"))
	if err != ErrTxClosed {
		t.Errorf("expected ErrTxClosed, got %v", err)
	}
	err = tx.Rollback()
	if err != ErrTxClosed {
		t.Errorf("expected ErrTxClosed, got %v", err)
	}
	err = tx.Commit()
	if err != ErrTxClosed {
		t.Errorf("expected ErrTxClosed, got %v", err)
	}

}

func TestTxRollback(t *testing.T) {
	defer os.RemoveAll("test")

	c, err := Open("test", testOptionsNoAutoMerge)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// create the first segment
	err = c.Update(func(tx *Tx) error {
		putKvPairs(tx, 0, 100)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("test/cellar-0000000000000001"); os.IsNotExist(err) {
		t.Errorf("expected cellar segment file 'test/cellar-0000000000000001' to exist, missing")
	}

	// start another segment, but abort it
	err = c.Update(func(tx *Tx) error {
		return fmt.Errorf("just feel like rollin back")
	})
	if _, err := os.Stat("test/cellar-0000000000000002"); err == nil {
		t.Errorf("expected cellar segment file 'test/cellar-0000000000000002' to be missing, it exists")
	}

	// create another segment
	err = c.Update(func(tx *Tx) error {
		putKvPairs(tx, 100, 200)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("test/cellar-0000000000000003"); os.IsNotExist(err) {
		t.Errorf("expected cellar segment file 'test/cellar-0000000000000003' to exist, missing")
	}

}
