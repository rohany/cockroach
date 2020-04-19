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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type createTypeNode struct {
	n *tree.CreateType
}

// Use to satisfy the linter.
var _ planNode = &createTypeNode{n: nil}

func (p *planner) CreateType(ctx context.Context, n *tree.CreateType) (planNode, error) {
	// TODO (rohany): Check permissions here.
	// TODO (rohany): Maybe error out here if there is already a type
	//  or table of the same name.
	return &createTypeNode{n: n}, nil
}

func (n *createTypeNode) startExec(params runParams) error {
	switch n.n.Variety {
	case tree.Enum:
		return params.p.createEnum(params, n.n)
	default:
		return unimplemented.NewWithIssue(25123, "CREATE TYPE")
	}
}

// getCreateTypeParams performs some initial validation on the input new
// TypeName and returns the key for the new type descriptor, the ID of the
// new type, the parent database and parent schema id.
func getCreateTypeParams(
	params runParams, name *tree.TypeName,
) (sqlbase.DescriptorKey, sqlbase.ID, *DatabaseDescriptor, sqlbase.ID, error) {
	db, err := params.p.ResolveUncachedDatabase(params.ctx, name)
	if err != nil {
		return nil, 0, nil, 0, err
	}
	if db.ID == keys.SystemDatabaseID {
		return nil, 0, nil, 0, errors.New("cannot create a type in the system database")
	}
	// TODO (rohany): This should be named object key.
	typeKey := sqlbase.MakePublicTableNameKey(params.ctx, params.ExecCfg().Settings, db.ID, name.Type())
	schemaID := sqlbase.ID(keys.PublicSchemaID)
	exists, _, err := sqlbase.LookupObjectID(params.ctx, params.p.txn, db.ID, schemaID, name.Type())
	if err == nil && exists {
		// TODO (rohany): We need to do an extra lookup to know if we collided
		//  with a type or a table.
		return nil, 0, nil, 0, sqlbase.NewRelationAlreadyExistsError(name.Type())
	}
	if err != nil {
		return nil, 0, nil, 0, err
	}
	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return nil, 0, nil, 0, err
	}
	return typeKey, id, db, schemaID, nil
}

func (p *planner) createEnum(params runParams, n *tree.CreateType) error {
	if len(n.EnumLabels) > 0 {
		return unimplemented.NewWithIssue(24873, "CREATE TYPE")
	}

	typeKey, id, db, schemaID, err := getCreateTypeParams(params, n.TypeName)
	if err != nil {
		return err
	}

	// TODO (rohany): We need to generate an oid for this type!
	typeDesc := &TypeDescriptor{
		ParentID:       db.ID,
		ParentSchemaID: schemaID,
		Name:           n.TypeName.Type(),
		ID:             id,
	}
	return p.createDescriptorWithID(
		params.ctx,
		typeKey.Key(),
		id,
		typeDesc,
		params.EvalContext().Settings,
		tree.AsStringWithFQNames(n, params.Ann()),
	)
}

func (n *createTypeNode) Next(params runParams) (bool, error) { return false, nil }
func (n *createTypeNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *createTypeNode) Close(ctx context.Context)           {}
func (n *createTypeNode) ReadingOwnWrites()                   {}
