// Copyright 2019 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type alterPrimaryKeyResumer struct {
	job *jobs.Job
	// TODO (rohany): should this be a table ID instead of a table descriptor?
	tableDesc        *TableDescriptor
	newPrimaryKey    *sqlbase.IndexDescriptor
	indexesToRewrite []*sqlbase.IndexDescriptor
}

type alterPrimaryKeyJobArgs struct {
	table            sqlbase.ID
	newPrimaryKey    sqlbase.IndexID
	indexesToRewrite []sqlbase.IndexID
}

var _ jobs.Resumer = &alterPrimaryKeyResumer{}

func startAlterPrimaryKeyJob(ctx context.Context, p *planner, args *alterPrimaryKeyJobArgs) error {
	record := jobs.Record{
		Username: p.User(),
		Details: jobspb.AlterPrimaryKeyDetails{
			TableID:          args.table,
			NewPkIndexID:     args.newPrimaryKey,
			IndexesToRewrite: args.indexesToRewrite,
		},
		Progress: jobspb.AlterPrimaryKeyProgress{},
	}
	// TODO (rohany): how do I use the error channel?
	// TODO (rohany): I don't think I need a results channel right?
	_, _, err := p.ExecCfg().JobRegistry.StartJob(ctx, make(chan tree.Datums, 1), record)
	// TODO (rohany): what to return here? some of the other jobs get something out of
	//  the errCh and return that -- is that what I'm supposed to do here?
	return err
}

func (a *alterPrimaryKeyResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	// TODO (rohany): as a first pass, lets not worry about the job restarting etc.
	// TODO (rohany): update this job with using a job details proto struct.
	fmt.Println("in the alterpk resumer")
	fmt.Println("%+v", a)
	p := phs.(*planner)
	details := a.job.Details().(jobspb.AlterPrimaryKeyDetails)
	fmt.Println("details %+v", details)
	// TODO (rohany): why is the planner's transaction nil?
	//  I can't use DB.Txn because creating indexes is not retryable.
	//  I could use it for the final writes at the end however.
	p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p.txn = txn
		rowEntry, err := p.LookupTableByID(ctx, details.TableID)
		if err != nil {
			return err
		}
		// TODO (rohany): this should be using protoutil clone
		tableDesc := sqlbase.NewMutableExistingTableDescriptor(*rowEntry.Desc.TableDesc())
		_ = tableDesc
		return err
	})
	return nil
}

func rewriteIndex(
	ctx context.Context,
	p *planner,
	table *MutableTableDescriptor,
	newPrimaryIndexID, targetIndexID sqlbase.IndexID,
) error {

	// Rewrite index proceeds as follows:
	// * Make a copy of the target index descriptor
	// * Update the new index descriptor's extra columns to be the columns in the new primary key.
	// * Enqueue the new index descriptor creation as a mutation onto the table descriptor.

	// TODO (rohany): handle indexes being dropped later
	newPrimaryIndex, err := table.FindIndexByID(newPrimaryIndexID)
	if err != nil {
		return err
	}
	targetIndex, err := table.FindIndexByID(targetIndexID)
	if err != nil {
		return err
	}
	newIndex := protoutil.Clone(targetIndex).(*sqlbase.IndexDescriptor)
	// Reset the ID of newIndex.
	newIndex.ID = 0

	mutationIdx := len(table.Mutations)
	if err := table.AddIndexMutation(newIndex, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := table.AllocateIDs(); err != nil {
		return err
	}

	// AllocateIDs might change some information about our index, so fetch it again.
	newIndex = table.Mutations[mutationIdx].GetIndex()
	// TODO (rohany): change this name to include a uuid or something?
	newIndex.Name = newIndex.Name + "rewrite_for_primary_key_change"
	// Use the columns in the new primary index to construct this indexes extra column ids list.
	newIndex.ExtraColumnIDs = nil
	var extraColumnIDs []sqlbase.ColumnID
	for _, colID := range newPrimaryIndex.ColumnIDs {
		if !newIndex.ContainsColumnID(colID) {
			extraColumnIDs = append(extraColumnIDs, colID)
		}
	}
	newIndex.ExtraColumnIDs = extraColumnIDs

	// TODO (rohany): interleaved tables will be ignored for now.

	// TODO (rohany): replace this with an AST statement.
	mutationID, err := p.createOrUpdateSchemaChangeJob(ctx, table, fmt.Sprintf("alter table %s alter primary key using %s@%s", table.Name, table.Name, newPrimaryIndex.Name))
	if err != nil {
		return err
	}

	if err := p.writeSchemaChange(ctx, table, mutationID); err != nil {
		return err
	}

	// TODO (rohany): we should also return the new index's ID here so we have both indexes
	//  to rewrite, and indexes to switch to -- maybe it should be a map? or just a list of tuples is fine.
	// TODO (rohany): add an event log entry for this.
	return MakeEventLogger(p.ExecCfg()).InsertEventRecord(
		ctx, p.Txn(), EventLogCreateIndex, int32(table.ID), int32(p.ExtendedEvalContext().NodeID),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{
			table.Name, newIndex.Name, fmt.Sprintf("alter table %s alter primary key using %s@%s", table.Name, table.Name, newPrimaryIndex.Name),
			p.User(), uint32(mutationID),
		},
	)
}

func (a *alterPrimaryKeyResumer) OnSuccess(ctx context.Context, txn *client.Txn) error {
	fmt.Println("in success")
	return nil
}

func (a *alterPrimaryKeyResumer) OnTerminal(
	ctx context.Context, status jobs.Status, resultsCh chan<- tree.Datums,
) {
	fmt.Println("in terminal!")
}

func (a *alterPrimaryKeyResumer) OnFailOrCancel(ctx context.Context, txn *client.Txn) error {
	fmt.Println("in fail or cancel")
	return nil
}

func init() {
	createAlterPrimaryKeyFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &alterPrimaryKeyResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAlterPrimaryKey, createAlterPrimaryKeyFn)
}
