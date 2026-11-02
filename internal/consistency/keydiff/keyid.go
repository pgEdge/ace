// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2026, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package keydiff

import (
	"encoding/binary"
	"fmt"
)

// encodeKeyID builds the key ID of a row from the text of each primary key
// column: for each column, the length of the text as an unsigned varint and
// then the bytes of the text. Two different lists of texts never give the same
// ID, whatever bytes the texts hold. A nil text is a NULL and gives an error:
// a primary key column cannot be NULL.
func encodeKeyID(texts [][]byte) (string, error) {
	size := 0
	for i, t := range texts {
		if t == nil {
			return "", fmt.Errorf("primary key column %d is NULL", i+1)
		}
		size += binary.MaxVarintLen64 + len(t)
	}
	buf := make([]byte, 0, size)
	for _, t := range texts {
		buf = binary.AppendUvarint(buf, uint64(len(t)))
		buf = append(buf, t...)
	}
	return string(buf), nil
}

// decodeKeyID splits a key ID back into the text of each of its n columns.
func decodeKeyID(id string, n int) ([]string, error) {
	out := make([]string, 0, n)
	b := []byte(id)
	for len(b) > 0 {
		l, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("bad key ID: wrong length prefix")
		}
		b = b[w:]
		if uint64(len(b)) < l {
			return nil, fmt.Errorf("bad key ID: value is shorter than its length")
		}
		out = append(out, string(b[:l]))
		b = b[l:]
	}
	if len(out) != n {
		return nil, fmt.Errorf("bad key ID: %d values, want %d", len(out), n)
	}
	return out, nil
}
