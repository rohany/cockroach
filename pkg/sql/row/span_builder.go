// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// SpanBuilder is a single struct for generating key spans from Constraints, Datums and encDatums.
type SpanBuilder struct {
	table         *sqlbase.TableDescriptor
	index         *sqlbase.IndexDescriptor
	indexColTypes []types.T
	indexColDirs  []sqlbase.IndexDescriptor_Direction

	keyPrefix []byte
	alloc     sqlbase.DatumAlloc

	// TODO (rohany): The interstices are used to convert opt constraints into spans. In future work,
	//  we should unify the codepaths and use the allocation free method used on datums.
	//  This work is tracked in #42738.

	neededFamilies []sqlbase.FamilyID
}

// Use some functions that aren't needed right now to make the linter happy.
var _ = (*SpanBuilder)(nil).UnsetNeededColumns
var _ = (*SpanBuilder)(nil).SpanFromDatumRow

// MakeSpanBuilder creates a SpanBuilder for a table and index.
func MakeSpanBuilder(table *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor) *SpanBuilder {
	s := &SpanBuilder{
		table:          table,
		index:          index,
		keyPrefix:      sqlbase.MakeIndexKeyPrefix(table, index.ID),
		neededFamilies: nil,
	}

	var columnIDs sqlbase.ColumnIDs
	columnIDs, s.indexColDirs = index.FullColumnIDs()
	s.indexColTypes = make([]types.T, len(columnIDs))
	for i, colID := range columnIDs {
		// TODO (rohany): do I need to look at table columns with mutations here as well?
		for _, col := range table.Columns {
			if col.ID == colID {
				s.indexColTypes[i] = col.Type
				break
			}
		}
	}

	return s
}

func (s *SpanBuilder) appendInterstice(key roachpb.Key, idx int) roachpb.Key {
	sharedPrefixLen := 0
	if idx == 0 {
		key = append(key, s.keyPrefix...)
	}
	if len(s.index.Interleave.Ancestors) > 0 {
		for i, ancestor := range s.index.Interleave.Ancestors {
			if i != 0 {
				if sharedPrefixLen == idx {
					key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
					key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
				}
			}
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
			if sharedPrefixLen == idx {
				key = encoding.EncodeInterleavedSentinel(key)
			}
		}
		if sharedPrefixLen == idx {
			key = encoding.EncodeUvarintAscending(key, uint64(s.table.ID))
			key = encoding.EncodeUvarintAscending(key, uint64(s.index.ID))
		}
	}
	return key
}

// SetNeededColumns sets the needed columns on the SpanBuilder. This information
// is used by MaybeSplitSpanIntoSeparateFamilies.
func (s *SpanBuilder) SetNeededColumns(neededCols util.FastIntSet) {
	s.neededFamilies = sqlbase.NeededColumnFamilyIDs(s.table.ColumnIdxMap(), s.table.Families, neededCols)
}

// UnsetNeededColumns resets the needed columns for column family specific optimizations
// that the SpanBuilder performs.
func (s *SpanBuilder) UnsetNeededColumns() {
	s.neededFamilies = nil
}

// SpanFromEncDatums encodes a span with prefixLen constraint columns from the index.
// SpanFromEncDatums assumes that the EncDatums in values are in the order of the index columns.
func (s *SpanBuilder) SpanFromEncDatums(
	values sqlbase.EncDatumRow, prefixLen int,
) (roachpb.Span, error) {
	return sqlbase.MakeSpanFromEncDatums(
		s.keyPrefix, values[:prefixLen], s.indexColTypes[:prefixLen], s.indexColDirs[:prefixLen], s.table, s.index, &s.alloc)
}

// SpanFromDatumRow generates an index span with prefixLen constraint columns from the index.
// SpanFromDatumRow assumes that values is a valid table row for the SpanBuilder's table.
func (s *SpanBuilder) SpanFromDatumRow(
	values tree.Datums, prefixLen int, colMap map[sqlbase.ColumnID]int,
) (roachpb.Span, error) {
	span, _, err := sqlbase.EncodePartialIndexSpan(s.table, s.index, prefixLen, colMap, values, s.keyPrefix)
	return span, err
}

// MaybeSplitSpanIntoSeparateFamilies uses the needed columns from SetNeededColumns to maybe split
// the input span into multiple family specific spans. prefixLen is the number of index columns
// encoded in the span.
func (s *SpanBuilder) MaybeSplitSpanIntoSeparateFamilies(
	span roachpb.Span, prefixLen int,
) roachpb.Spans {
	if s.neededFamilies != nil && s.canSplitSpanIntoSeparateFamilies(s.neededFamilies, prefixLen) {
		return sqlbase.SplitSpanIntoSeparateFamilies(span, s.neededFamilies)
	}
	return roachpb.Spans{span}
}

func (s *SpanBuilder) canSplitSpanIntoSeparateFamilies(
	neededFamilies []sqlbase.FamilyID, prefixLen int,
) bool {
	// Right now, we can/should only split a span into separate family point lookups if:
	// * the table has more than one family
	// * the index is the primary key
	// * we have all of the columns of the index
	// * we don't need all of the families
	return len(s.table.Families) > 1 &&
		s.index.ID == s.table.PrimaryIndex.ID &&
		prefixLen == len(s.index.ColumnIDs) &&
		len(neededFamilies) < len(s.table.Families)
}

// Functions for optimizer related span generation are below.

// SpansFromConstraint generates spans from an optimizer constraint.
// TODO (rohany): In future work, there should be a single API to generate spans
//  from constraints, datums and encdatums.
func (s *SpanBuilder) SpansFromConstraint(
	c *constraint.Constraint, needed util.FastIntSet, forDelete bool,
) (roachpb.Spans, error) {
	var spans roachpb.Spans
	var err error
	if c == nil || c.IsUnconstrained() {
		// Encode a full span.
		spans, err = s.appendSpansFromConstraintSpan(spans, &constraint.UnconstrainedSpan, needed, forDelete)
		if err != nil {
			return nil, err
		}
		return spans, nil
	}

	spans = make(roachpb.Spans, 0, c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		spans, err = s.appendSpansFromConstraintSpan(spans, c.Spans.Get(i), needed, forDelete)
		if err != nil {
			return nil, err
		}
	}
	return spans, nil
}

// UnconstrainedSpans returns the full span corresponding to the SpanBuilder's
// table and index.
func (s *SpanBuilder) UnconstrainedSpans(forDelete bool) (roachpb.Spans, error) {
	return s.SpansFromConstraint(nil, exec.ColumnOrdinalSet{}, forDelete)
}

// appendSpansFromConstraintSpan converts a constraint.Span to one or more
// roachpb.Spans and appends them to the provided spans. It appends multiple
// spans in the case that multiple, non-adjacent column families should be
// scanned. The forDelete parameter indicates whether these spans will be used
// for row deletion.
func (s *SpanBuilder) appendSpansFromConstraintSpan(
	spans roachpb.Spans, cs *constraint.Span, needed util.FastIntSet, forDelete bool,
) (roachpb.Spans, error) {
	var span roachpb.Span
	var err error
	// Encode each logical part of the start key.
	span.Key, err = s.encodeConstraintKey(cs.StartKey())
	if err != nil {
		return nil, err
	}
	if cs.StartBoundary() == constraint.IncludeBoundary {
		span.Key = s.appendInterstice(span.Key, cs.StartKey().Length())
	} else {
		// We need to exclude the value this logical part refers to.
		span.Key = span.Key.PrefixEnd()
	}
	// Encode each logical part of the end key.
	span.EndKey, err = s.encodeConstraintKey(cs.EndKey())
	if err != nil {
		return nil, err
	}
	span.EndKey = s.appendInterstice(span.EndKey, cs.EndKey().Length())

	// Optimization: for single row lookups on a table with multiple column
	// families, only scan the relevant column families. This is disabled for
	// deletions to ensure that the entire row is deleted.
	if !forDelete && needed.Len() > 0 && span.Key.Equal(span.EndKey) {
		neededFamilyIDs := sqlbase.NeededColumnFamilyIDs(s.table.ColumnIdxMap(), s.table.Families, needed)
		if s.canSplitSpanIntoSeparateFamilies(neededFamilyIDs, cs.StartKey().Length()) {
			return append(spans, sqlbase.SplitSpanIntoSeparateFamilies(span, neededFamilyIDs)...), nil
		}
	}

	// We tighten the end key to prevent reading interleaved children after the
	// last parent key. If cs.End.Inclusive is true, we also advance the key as
	// necessary.
	endInclusive := cs.EndBoundary() == constraint.IncludeBoundary
	span.EndKey, err = sqlbase.AdjustEndKeyForInterleave(s.table, s.index, span.EndKey, endInclusive)
	if err != nil {
		return nil, err
	}
	return append(spans, span), nil
}

// encodeConstraintKey encodes each logical part of a constraint.Key into a roachpb.Key.
func (s *SpanBuilder) encodeConstraintKey(ck constraint.Key) (roachpb.Key, error) {
	if s.index.Type == sqlbase.IndexDescriptor_INVERTED {
		var key []byte
		// TODO (rohany): is it always the case that that an inverted index is not interleaved?
		//  also, will an inverted index have multiple values???
		for i := 0; i < ck.Length(); i++ {
			key = s.appendInterstice(key, i)
			keys, err := sqlbase.EncodeInvertedIndexTableKeys(ck.Value(i), key)
			if err != nil {
				return nil, err
			}
			if len(keys) > 1 {
				err := errors.AssertionFailedf("trying to use multiple keys in index lookup")
				return nil, err
			}
			key = keys[0]
		}
		return key, nil
	} else {
		return s.encodeConstraintKey(ck)
	}
}

func (s *SpanBuilder) encodeConstraintKeyNoInterstices(ck constraint.Key) (roachpb.Key, error) {
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key := make(roachpb.Key, len(s.keyPrefix), len(s.keyPrefix)*2)
	copy(key, s.keyPrefix)

	startIndex := 0
	if len(s.index.Interleave.Ancestors) > 0 {
		for i, ancestor := range s.index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
			}
			partial := false
			length := int(ancestor.SharedPrefixLen)
			if length > ck.Length() {
				length = ck.Length()
				partial = true
			}
			var err error
			key, err = s.appendConstraintKey(key, ck, startIndex, length)
			if err != nil {
				return nil, err
			}
			if partial {
				// Early stop - the number of desired columns was fewer than the number
				// left in the current interleave.
				return key, nil
			}
			startIndex += length
			key = encoding.EncodeInterleavedSentinel(key)
		}
		key = encoding.EncodeUvarintAscending(key, uint64(s.table.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(s.index.ID))
	}
	return s.appendConstraintKey(key, ck, startIndex, ck.Length())
}

func (s *SpanBuilder) appendConstraintKey(
	key roachpb.Key, ck constraint.Key, start, end int,
) (roachpb.Key, error) {
	var err error
	for i := start; i < end; i++ {
		dir := encoding.Ascending
		if i < len(s.index.ColumnDirections) {
			dir, err = s.index.ColumnDirections[i].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		}
		key, err = sqlbase.EncodeTableKey(key, ck.Value(i), dir)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}
