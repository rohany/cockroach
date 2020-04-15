// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type createTypeNode struct {
	n *tree.CreateType
}

// Use to satisfy the linter.
var _ planNode = &createTypeNode{n: nil}

func (p *planner) CreateType(ctx context.Context, n *tree.CreateType) (planNode, error) {
	// TODO (rohany): check privilege here.
	return &createTypeNode{n: n}, nil
}

func (n *createTypeNode) startExec(params runParams) error {

	tblName := n.n.TypeName.ToTableName()
	db, err := params.p.ResolveUncachedDatabase(params.ctx, &tblName)
	if err != nil {
		return err
	}
	// TODO (rohany): This needs to resolve the schema id ?
	exists, _, err := sqlbase.LookupObjectID(params.ctx, params.p.txn, db.ID, keys.PublicSchemaID, tblName.Table())
	if err == nil && exists {
		// TODO (rohany): This should return whether there is a conflicting type or table.
		//  Though that seems like it would incur another lookup.
		return sqlbase.NewRelationAlreadyExistsError(tblName.Table())
	}
	if err != nil {
		return err
	}

	typeKey := sqlbase.NewTableKey(db.ID, keys.PublicSchemaID, tblName.Table())
	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	typeDesc := &sqlbase.TypeDescriptor{
		ParentID:       db.ID,
		ParentSchemaID: keys.PublicSchemaID,
		Name:           tblName.Table(),
		ID:             id,
	}

	if err := params.p.createDescriptorWithID(params.ctx, typeKey.Key(), id, typeDesc, params.EvalContext().Settings, tree.AsStringWithFQNames(n.n, params.Ann())); err != nil {
		return err
	}

	return nil
}

func (n *createTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createTypeNode) Close(ctx context.Context)           {}
func (n *createTypeNode) ReadingOwnWrites()                   {}
