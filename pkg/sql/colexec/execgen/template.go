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
	"bytes"
	"go/parser"
	"go/token"
	"regexp"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
	"github.com/dave/dst/dstutil"
)

func TemplatizeFuncs(contents string) (string, error) {
	f, err := decorator.ParseFile(token.NewFileSet(), "", contents, parser.ParseComments)
	if err != nil {
		return "", err
	}
	//templateFuncMap := make(map[string]funcInfo)
	//
	//// First, run over the input file, searching for functions that are annotated
	//// with execgen:inline.
	//n := extractInlineFuncDecls(f, templateFuncMap)

	//fmt.Println(n)
	//fmt.Println(templateFuncMap)

	templateFuncs := make(map[string]funcInfo)

	// Generate all templatized functions.
	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.FuncDecl:
			var templateVars []string
			for _, dec := range n.Decorations().Start.All() {
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					match := matches[1]
					// Match now looks like foo, bar
					templateVars = strings.Split(match, ",")
					for i, v := range templateVars {
						templateVars[i] = strings.TrimSpace(v)
					}
				}
			}
			if len(templateVars) == 0 {
				return true
			}

			var templateParams []templateParamInfo
			for _, v := range templateVars {
				for i, f := range n.Type.Params.List {
					if v == f.Names[0].Name {
						templateParams = append(templateParams, templateParamInfo{
							fieldOrdinal: i,
							field:        dst.Clone(f).(*dst.Field),
						})
						break
					}
				}
			}

			// Generate a monomorphised version of the function without the template args.
			trueFunc := dst.Clone(n).(*dst.FuncDecl)
			// Remove the template arguments.
			newParamList := make([]*dst.Field, 0, len(n.Type.Params.List)-len(templateParams))
			for i, field := range trueFunc.Type.Params.List {
				var skip bool
				for _, p := range templateParams {
					if i == p.fieldOrdinal {
						skip = true
						break
					}
				}
				if !skip {
					newParamList = append(newParamList, field)
				}
			}
			trueFunc.Type.Params.List = newParamList
			for _, s := range templateVars {
				trueFunc.Name.Name = trueFunc.Name.Name + "_" + s + "_true"
			}
			// Remove the template parameter.
			decs := trueFunc.Decorations().Start
			for i, dec := range decs.All() {
				if matches := templateRe.FindStringSubmatch(dec); matches != nil {
					decs[i] = ""
				} else if strings.Contains(dec, "{{") {
					decs[i] = ""
				}
			}

			dstutil.Apply(trueFunc.Body, func(cursor *dstutil.Cursor) bool {
				n := cursor.Node()
				switch n := n.(type) {
				case *dst.IfStmt:
					switch c := n.Cond.(type) {
					case *dst.Ident:
						for _, p := range templateParams {
							if ident, ok := p.field.Type.(*dst.Ident); !ok || ident.Name != "bool" {
								// Can only template bool types right now.
								continue
							}
							if c.Name == p.field.Names[0].Name {
								for _, stmt := range n.Body.List {
									cursor.InsertAfter(stmt)
								}
								cursor.Delete()
								return true
							}
						}
					}
				}
				return true
			}, nil)
			cursor.InsertAfter(trueFunc)

			templateFuncs[n.Name.Name] = funcInfo{
				decl:           trueFunc,
				templateParams: templateParams,
			}
		}
		return true
	}, nil)
	// Replace call sites with calls to templatized versions.
	dstutil.Apply(f, func(cursor *dstutil.Cursor) bool {
		n := cursor.Node()
		switch n := n.(type) {
		case *dst.ExprStmt:
			callExpr, ok := n.X.(*dst.CallExpr)
			if !ok {
				return true
			}
			funcIdent, ok := callExpr.Fun.(*dst.Ident)
			if !ok {
				return true
			}
			trueFunc, ok := templateFuncs[funcIdent.Name]
			if !ok {
				return true
			}

			// From the arguments, see if we can switch to a templated call.
			for _, p := range trueFunc.templateParams {
				if ident, ok := callExpr.Args[p.fieldOrdinal].(*dst.Ident); ok && ident.Name == "true" {
					callExpr.Fun.(*dst.Ident).Name = trueFunc.decl.Name.Name
					// Splice out the template argument from the call.
					callExpr.Args = append(callExpr.Args[:p.fieldOrdinal:p.fieldOrdinal], callExpr.Args[p.fieldOrdinal+1:]...)
					break
				}
			}
		}
		return true
	}, nil)

	// decorator.Fprint(os.Stdout, f)

	b := bytes.Buffer{}
	_ = decorator.Fprint(&b, f)
	return b.String(), nil
}

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
