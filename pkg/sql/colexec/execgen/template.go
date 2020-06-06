// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"regexp"

	"github.com/dave/dst"
)

type templateParamInfo struct {
	fieldOrdinal int
	field        *dst.Field
	instances    []*dst.Expr
}

type funcInfo struct {
	decl           *dst.FuncDecl
	templateParams []templateParamInfo
}

// Match // execgen:template<foo, bar>
var templateRe = regexp.MustCompile(`\/\/ *execgen:template<((?:(?:\w+),?\W*)+)>`)

func replaceTemplateVars(
	info *funcInfo, call *dst.CallExpr,
) (templateArgs []dst.Expr, newCall *dst.CallExpr) {
	if len(info.templateParams) == 0 {
		return nil, call
	}
	templateArgs = make([]dst.Expr, len(info.templateParams))
	// Collect template arguments.
	for i, param := range info.templateParams {
		templateArgs[i] = dst.Clone(call.Args[param.fieldOrdinal]).(dst.Expr)
	}
	// Remove template vars from callsite.
	newArgs := make([]dst.Expr, 0, len(call.Args)-len(info.templateParams))
	for i := range call.Args {
		skip := false
		for _, p := range info.templateParams {
			if p.fieldOrdinal == i {
				skip = true
				break
			}
		}
		if !skip {
			newArgs = append(newArgs, call.Args[i])
		}
	}
	ret := dst.Clone(call).(*dst.CallExpr)
	ret.Args = newArgs
	return templateArgs, ret
}
