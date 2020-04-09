// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type TypeReferenceResolver interface {
	ResolveType() (*types.T, error)
}

type ResolvableTypeReference interface {
	fmt.Stringer
	NodeFormatter
	typeReference()
	Resolve(resolver TypeReferenceResolver) (*types.T, error)
}

type KnownType struct {
	*types.T
}

func MakeKnownType(typ *types.T) ResolvableTypeReference {
	return &KnownType{typ}
}

// Format implements the NodeFormatter interface.
func (node *KnownType) Format(ctx *FmtCtx) {
	ctx.WriteString(node.SQLString())
}

func (ty *KnownType) Resolve(resolver TypeReferenceResolver) (*types.T, error) {
	return ty.T, nil
}

func (name *UnresolvedName) Resolve(resolver TypeReferenceResolver) (*types.T, error) {
	return nil, nil
}

var _ ResolvableTypeReference = &KnownType{}
var _ ResolvableTypeReference = &UnresolvedName{}

func (*KnownType) typeReference()      {}
func (*UnresolvedName) typeReference() {}
