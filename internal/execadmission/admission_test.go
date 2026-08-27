package execadmission

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"cel.dev/cel-go/cel"
	celtypes "cel.dev/cel-go/common/types"
	"cel.dev/cel-go/common/types/ref"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
	sandboxproto "github.com/kuasar-sandbox/sandboxer/pkg/proto"
)

func TestCompilerFixedRequestView(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	conditions := []string{
		`request.argv == ['/usr/bin/python3', '/workspace/task.py']`,
		`request.env['LANG'] == 'C.UTF-8'`,
		`request.cwd == '/workspace'`,
		`request.user == '1000:1000'`,
		`!request.stdio.tty && request.stdio.stdin && request.stdio.stdout && request.stdio.stderr`,
	}
	set, err := compiler.Compile(conditions)
	if err != nil {
		t.Fatal(err)
	}
	spec := &sandboxproto.ExecSpec{
		Argv: []string{"/usr/bin/python3", "/workspace/task.py"},
		Env:  map[string]string{"LANG": "C.UTF-8"},
		Cwd:  "/workspace",
		User: "1000:1000",
		Stdio: sandboxproto.StdioSpec{
			Stdin: true, Stdout: true, Stderr: true,
		},
	}
	if err := set.Evaluate(context.Background(), spec); err != nil {
		t.Fatalf("Evaluate() = %v", err)
	}
	spec.User = "0:0"
	if err := set.Evaluate(context.Background(), spec); !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("AND rejection = %v", err)
	}
}

func TestCompilerSupportsCallerOR(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	set, err := compiler.Compile([]string{
		`request.argv[0] == '/bin/true' || request.argv[0] == '/usr/bin/true'`,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, argv := range [][]string{{"/bin/true"}, {"/usr/bin/true"}} {
		if err := set.Evaluate(context.Background(), &sandboxproto.ExecSpec{Argv: argv}); err != nil {
			t.Fatalf("argv %q rejected: %v", argv, err)
		}
	}
	if err := set.Evaluate(context.Background(), &sandboxproto.ExecSpec{Argv: []string{"/bin/false"}}); !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("false OR = %v", err)
	}
}

func TestCompilerNormalizationAndTTYIgnoredSemantics(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	set, err := compiler.Compile([]string{
		`request.argv == []`,
		`request.env == {}`,
		`request.cwd == '/'`,
		`request.user == ''`,
		`request.stdio.tty && !request.stdio.stdin && !request.stdio.stdout && !request.stdio.stderr`,
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := &sandboxproto.ExecSpec{Stdio: sandboxproto.StdioSpec{
		TTY: true, Stdin: true, Stdout: true, Stderr: true,
	}}
	if err := set.Evaluate(context.Background(), spec); err != nil {
		t.Fatalf("normalized TTY request = %v", err)
	}

	root, err := compiler.Compile([]string{`request.cwd == '/'`})
	if err != nil {
		t.Fatal(err)
	}
	for _, cwd := range []string{"", "/"} {
		if err := root.Evaluate(context.Background(), &sandboxproto.ExecSpec{Cwd: cwd}); err != nil {
			t.Fatalf("cwd %q = %v", cwd, err)
		}
	}
}

func TestCompilerRejectsInvalidTypesAndFailsClosed(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	for name, expression := range map[string]string{
		"unknown field":  `request.metadata == {}`,
		"non bool":       `request.cwd`,
		"unknown nested": `request.stdio.winsize == null`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := compiler.Compile([]string{expression}); !errors.Is(err, ErrInvalidCondition) {
				t.Fatalf("Compile() = %v", err)
			}
		})
	}

	for name, expression := range map[string]string{
		"false":         `request.cwd == '/denied'`,
		"runtime error": `request.argv[99] == 'missing'`,
	} {
		t.Run(name, func(t *testing.T) {
			set, err := compiler.Compile([]string{expression})
			if err != nil {
				t.Fatal(err)
			}
			if err := set.Evaluate(context.Background(), &sandboxproto.ExecSpec{Argv: []string{"true"}}); !errors.Is(err, ErrRequestRejected) {
				t.Fatalf("Evaluate() = %v", err)
			}
		})
	}

	set, err := compiler.Compile([]string{`request.argv.exists(arg, arg == 'never')`})
	if err != nil {
		t.Fatal(err)
	}
	argv := make([]string, limits.MaxRuntimeCost+1)
	for index := range argv {
		argv[index] = "allowed-input"
	}
	if err := set.Evaluate(context.Background(), &sandboxproto.ExecSpec{Argv: argv}); !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("cost-exceeded Evaluate() = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := set.Evaluate(canceled, &sandboxproto.ExecSpec{}); !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("canceled Evaluate() = %v", err)
	}

	unknown := &ProgramSet{programs: []cel.Program{unknownTestProgram{}}, types: compiler.types}
	if err := unknown.Evaluate(context.Background(), &sandboxproto.ExecSpec{}); !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("unknown Evaluate() = %v", err)
	}
}

type unknownTestProgram struct{}

func (unknownTestProgram) Eval(any) (ref.Val, *cel.EvalDetails, error) {
	return celtypes.NewUnknown(1, nil), nil, nil
}

func (unknownTestProgram) ContextEval(context.Context, any) (ref.Val, *cel.EvalDetails, error) {
	return celtypes.NewUnknown(1, nil), nil, nil
}

func (unknownTestProgram) ConcurrentEval(context.Context, any) <-chan cel.EvalResult { return nil }

func TestCompilerUnrestricted(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	for _, conditions := range [][]string{nil, {}} {
		set, err := compiler.Compile(conditions)
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Evaluate(context.Background(), nil); err != nil {
			t.Fatalf("unrestricted Evaluate() = %v", err)
		}
	}
}

func TestCompilerEnforcesSourceAndComplexityBounds(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	tooMany := make([]string, limits.MaxConditions+1)
	for index := range tooMany {
		tooMany[index] = "true"
	}
	total := make([]string, 5)
	for index := range total {
		total[index] = strings.Repeat(" ", 899) + "true"
	}
	deepAST := "true"
	for range limits.MaxASTDepth + 1 {
		deepAST = "true?" + deepAST + ":false"
	}
	deepComprehension := "true"
	for index := range limits.MaxComprehensionDepth + 1 {
		deepComprehension = fmt.Sprintf("request.argv.exists(x%d,%s)", index, deepComprehension)
	}
	tests := map[string][]string{
		"condition count":     tooMany,
		"expression bytes":    {strings.Repeat("!", limits.MaxConditionExprBytes) + "true"},
		"total bytes":         total,
		"AST depth":           {deepAST},
		"comprehension depth": {deepComprehension},
		"static cost":         {`request.argv.exists(x, request.argv.exists(y, x == y))`},
	}
	for name, conditions := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := compiler.Compile(conditions); !errors.Is(err, ErrInvalidCondition) {
				t.Fatalf("Compile() = %v, want invalid condition", err)
			}
		})
	}
}

func TestCompilerCacheHitEvictionAndConcurrentSingleflight(t *testing.T) {
	compiler, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	compiler.cache = newProgramCache(2)

	first, err := compiler.Compile([]string{`request.cwd == '/one'`})
	if err != nil {
		t.Fatal(err)
	}
	again, err := compiler.Compile([]string{`request.cwd == '/one'`})
	if err != nil || first != again || compiler.compileCount != 1 {
		t.Fatalf("cache hit set=%p/%p compileCount=%d err=%v", first, again, compiler.compileCount, err)
	}
	if _, err := compiler.Compile([]string{`request.cwd == '/two'`}); err != nil {
		t.Fatal(err)
	}
	if _, err := compiler.Compile([]string{`request.cwd == '/three'`}); err != nil {
		t.Fatal(err)
	}
	if compiler.cache.Len() != 2 {
		t.Fatalf("cache len = %d", compiler.cache.Len())
	}
	if _, err := compiler.Compile([]string{`request.cwd == '/one'`}); err != nil {
		t.Fatal(err)
	}
	if compiler.compileCount != 4 {
		t.Fatalf("compile count after eviction = %d, want 4", compiler.compileCount)
	}

	concurrent, err := NewCompiler()
	if err != nil {
		t.Fatal(err)
	}
	const workers = 32
	start := make(chan struct{})
	results := make(chan *ProgramSet, workers)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			set, err := concurrent.Compile([]string{`request.user == '1000:1000'`})
			results <- set
			errorsCh <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var want *ProgramSet
	for set := range results {
		if want == nil {
			want = set
		} else if set != want {
			t.Fatal("singleflight callers received different cached sets")
		}
	}
	if concurrent.compileCount != 1 {
		t.Fatalf("concurrent compile count = %d, want 1", concurrent.compileCount)
	}
}

func BenchmarkCompileConditions(b *testing.B) {
	compiler, err := NewCompiler()
	if err != nil {
		b.Fatal(err)
	}
	conditions := typicalConditions()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiler.compileUncached(conditions); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompileCacheHit(b *testing.B) {
	compiler, err := NewCompiler()
	if err != nil {
		b.Fatal(err)
	}
	conditions := typicalConditions()
	if _, err := compiler.Compile(conditions); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := compiler.Compile(conditions); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEvaluateConditions(b *testing.B) {
	compiler, err := NewCompiler()
	if err != nil {
		b.Fatal(err)
	}
	set, err := compiler.Compile(typicalConditions())
	if err != nil {
		b.Fatal(err)
	}
	spec := benchmarkExecSpec()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := set.Evaluate(context.Background(), spec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEvaluateUnrestricted(b *testing.B) {
	compiler, err := NewCompiler()
	if err != nil {
		b.Fatal(err)
	}
	set, err := compiler.Compile(nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := set.Evaluate(context.Background(), nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMaximumConditionSet(b *testing.B) {
	compiler, err := NewCompiler()
	if err != nil {
		b.Fatal(err)
	}
	conditions := make([]string, limits.MaxConditions)
	for index := range conditions {
		conditions[index] = fmt.Sprintf("request.argv.size() >= 0 && %d >= 0", index)
	}
	set, err := compiler.Compile(conditions)
	if err != nil {
		b.Fatal(err)
	}
	spec := benchmarkExecSpec()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := set.Evaluate(context.Background(), spec); err != nil {
			b.Fatal(err)
		}
	}
}

func typicalConditions() []string {
	return []string{
		`request.argv == ['/usr/bin/python3', '/workspace/task.py']`,
		`request.cwd == '/workspace'`,
		`request.user == '1000:1000' && !request.stdio.tty`,
	}
}

func benchmarkExecSpec() *sandboxproto.ExecSpec {
	return &sandboxproto.ExecSpec{
		Argv: []string{"/usr/bin/python3", "/workspace/task.py"},
		Env:  map[string]string{"LANG": "C.UTF-8"},
		Cwd:  "/workspace", User: "1000:1000",
	}
}
