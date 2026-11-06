// Package archtest holds tests about the shape of the code rather than its behaviour.
package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// clockReads are the time functions that read the wall clock.
var clockReads = map[string]bool{"Now": true, "Since": true, "Until": true}

const rule = "production code must read the clock only through a `now` field " +
	"(`now: time.Now` or `now: func() time.Time { return time.Now().UTC() }`), so tests can pin time"

// clockViolations returns "file:line: time.X" for every clock read in file that is not the
// value of a composite-literal key `now` or of an assignment to a `.now` field. That value must
// be the function itself (`time.Now`) or a func literal; `now: time.Now()` stores one reading.
func clockViolations(fset *token.FileSet, file *ast.File) []string {
	timePkg := timeImportName(file)
	if timePkg == "" {
		return nil
	}

	var allowed []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && key.Name == "now" && isClockFunc(n.Value, timePkg) {
				allowed = append(allowed, n.Value)
			}
		case *ast.AssignStmt:
			for i, lhs := range n.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "now" && i < len(n.Rhs) && isClockFunc(n.Rhs[i], timePkg) {
					allowed = append(allowed, n.Rhs[i])
				}
			}
		}
		return true
	})
	inside := func(n ast.Node) bool {
		for _, a := range allowed {
			if a.Pos() <= n.Pos() && n.End() <= a.End() {
				return true
			}
		}
		return false
	}

	var found []string
	ast.Inspect(file, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || !clockReads[sel.Sel.Name] {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == timePkg && !inside(sel) {
			found = append(found, fmt.Sprintf("%s: time.%s", fset.Position(sel.Pos()), sel.Sel.Name))
		}
		return true
	})
	return found
}

// isClockFunc reports whether e is a clock function rather than one reading of the clock:
// the bare `time.Now` method value, or a func literal (read each time it is called).
func isClockFunc(e ast.Expr, timePkg string) bool {
	switch e := e.(type) {
	case *ast.FuncLit:
		return true
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		return ok && pkg.Name == timePkg && e.Sel.Name == "Now"
	}
	return false
}

// timeImportName is the local name of the "time" import, or "" when the file does not import it.
func timeImportName(file *ast.File) string {
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != "time" {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return "time"
	}
	return ""
}

func parseFixture(t *testing.T, name string) (*token.FileSet, *ast.File) {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return fset, file
}

// The rule must catch every way a clock read can slip past the seam; otherwise the tree test
// below proves nothing.
func TestClockRuleCatchesEveryBypass(t *testing.T) {
	fset, file := parseFixture(t, "bad.go.txt")
	got := clockViolations(fset, file)
	want := []string{
		"bad.go.txt:11:6: time.Now",
		"bad.go.txt:12:6: time.Since",
		"bad.go.txt:13:12: time.Now",
		"bad.go.txt:14:20: time.Now",
		"bad.go.txt:18:18: time.Now",
		"bad.go.txt:19:36: time.Now",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("violations in the bad fixture:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestClockRuleAllowsTheSeam(t *testing.T) {
	fset, file := parseFixture(t, "good.go.txt")
	if got := clockViolations(fset, file); len(got) > 0 {
		t.Errorf("the allowed seam forms were flagged:\n%s", strings.Join(got, "\n"))
	}
}

// TestProductionCodeReadsTheClockOnlyThroughNow scans every non-test Go file of the module
// outside testdata.
func TestProductionCodeReadsTheClockOnlyThroughNow(t *testing.T) {
	root := filepath.Join("..", "..")
	fset := token.NewFileSet()
	var found []string
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && d.Name() == "testdata" {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			found = append(found, clockViolations(fset, file)...)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) > 0 {
		t.Errorf("%s. Violations:\n%s", rule, strings.Join(found, "\n"))
	}
}
