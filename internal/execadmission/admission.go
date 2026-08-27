// Package execadmission compiles and evaluates the fixed exec-condition CEL
// environment shared by the Router and final node proxy.
package execadmission

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/checker"
	celtypes "cel.dev/cel-go/common/types"
	"golang.org/x/sync/singleflight"
	"google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
	sandboxproto "github.com/kuasar-sandbox/sandboxer/pkg/proto"
)

var (
	ErrInvalidCondition = errors.New("invalid exec condition")
	ErrRequestRejected  = errors.New("exec request rejected")

	defaultOnce     sync.Once
	defaultCompiler *Compiler
	defaultErr      error
)

// Compiler owns the fixed CEL environment and a bounded concurrent cache.
type Compiler struct {
	env   *cel.Env
	types *requestTypes
	cache *programCache
	group singleflight.Group

	compileMu    sync.Mutex
	compileCount uint64
}

// ProgramSet is an immutable ordered AND-set of compiled CEL programs.
type ProgramSet struct {
	programs []cel.Program
	types    *requestTypes
}

// NewCompiler constructs the fixed strongly typed request environment.
func NewCompiler() (*Compiler, error) {
	types, err := newRequestTypes()
	if err != nil {
		return nil, err
	}
	env, err := cel.NewEnv(
		cel.TypeDescs(types.requestType.Descriptor().ParentFile()),
		cel.Variable("request", cel.ObjectType(string(requestMessageName))),
		cel.ParserExpressionSizeLimit(limits.MaxConditionExprBytes),
		cel.ParserRecursionLimit(limits.MaxASTDepth),
		cel.ExpressionNestingDepthLimit(limits.MaxASTDepth),
		cel.ExpressionNodeLimit(limits.MaxASTNodes),
	)
	if err != nil {
		return nil, fmt.Errorf("create exec admission environment: %w", err)
	}
	return &Compiler{
		env: env, types: types,
		cache: newProgramCache(limits.ProgramCacheCapacity),
	}, nil
}

// Default returns the process-wide compiler/cache shared by all exec gates.
func Default() (*Compiler, error) {
	defaultOnce.Do(func() {
		defaultCompiler, defaultErr = NewCompiler()
	})
	return defaultCompiler, defaultErr
}

// Compile validates and compiles ordered conditions. Empty input returns an
// unrestricted set. Errors never include expression source.
func (c *Compiler) Compile(expressions []string) (*ProgramSet, error) {
	normalized, err := limits.NormalizeExpressions(expressions)
	if err != nil {
		return nil, ErrInvalidCondition
	}
	if len(normalized) == 0 {
		return &ProgramSet{types: c.types}, nil
	}
	key := conditionKey(normalized)
	if cached := c.cache.Get(key); cached != nil {
		return cached, nil
	}
	value, err, _ := c.group.Do(key, func() (any, error) {
		if cached := c.cache.Get(key); cached != nil {
			return cached, nil
		}
		compiled, err := c.compileUncached(normalized)
		if err != nil {
			return nil, err
		}
		c.cache.Add(key, compiled)
		return compiled, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(*ProgramSet), nil
}

func (c *Compiler) compileUncached(expressions []string) (*ProgramSet, error) {
	c.compileMu.Lock()
	c.compileCount++
	c.compileMu.Unlock()

	programs := make([]cel.Program, 0, len(expressions))
	estimator := boundedCostEstimator{}
	for i, expression := range expressions {
		ast, issues := c.env.Compile(expression)
		if issues != nil && issues.Err() != nil {
			return nil, conditionError(i)
		}
		if ast == nil || ast.OutputType() != cel.BoolType ||
			comprehensionDepth(ast.Expr(), 0) > limits.MaxComprehensionDepth {
			return nil, conditionError(i)
		}
		cost, err := c.env.EstimateCost(ast, estimator)
		if err != nil || cost.Max > limits.MaxStaticCost {
			return nil, conditionError(i)
		}
		program, err := c.env.Program(
			ast,
			cel.CostLimit(limits.MaxRuntimeCost),
			cel.InterruptCheckFrequency(1),
		)
		if err != nil {
			return nil, conditionError(i)
		}
		programs = append(programs, program)
	}
	return &ProgramSet{programs: programs, types: c.types}, nil
}

func conditionError(index int) error {
	return fmt.Errorf("%w at index %d", ErrInvalidCondition, index)
}

// Evaluate applies all conditions in order with AND semantics. False, CEL
// errors, unknown values, cost exhaustion, and cancellation fail closed.
func (set *ProgramSet) Evaluate(ctx context.Context, spec *sandboxproto.ExecSpec) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrRequestRejected
	}
	if set == nil {
		return ErrRequestRejected
	}
	if len(set.programs) == 0 {
		return nil
	}
	if set.types == nil {
		return ErrRequestRejected
	}
	request := messageForTypes(set.types, spec)
	activation := map[string]any{"request": request}
	for _, program := range set.programs {
		if err := ctx.Err(); err != nil {
			return ErrRequestRejected
		}
		value, _, err := program.ContextEval(ctx, activation)
		if err != nil || value == nil || celtypes.IsError(value) || celtypes.IsUnknown(value) {
			return ErrRequestRejected
		}
		allowed, ok := value.Value().(bool)
		if !ok || !allowed {
			return ErrRequestRejected
		}
	}
	return nil
}

func messageForTypes(types *requestTypes, spec *sandboxproto.ExecSpec) *dynamicpb.Message {
	request := dynamicpb.NewMessage(types.requestType.Descriptor())
	argv := request.Mutable(types.argv).List()
	env := request.Mutable(types.env).Map()
	cwd, user := "/", ""
	var stdio sandboxproto.StdioSpec
	if spec != nil {
		for _, arg := range spec.Argv {
			argv.Append(protoreflect.ValueOfString(arg))
		}
		for key, value := range spec.Env {
			env.Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfString(value))
		}
		if spec.Cwd != "" && spec.Cwd != "/" {
			cwd = spec.Cwd
		}
		user = spec.User
		stdio = spec.Stdio
	}
	request.Set(types.cwd, protoreflect.ValueOfString(cwd))
	request.Set(types.user, protoreflect.ValueOfString(user))
	stdioMessage := dynamicpb.NewMessage(types.stdioType.Descriptor())
	stdioMessage.Set(types.tty, protoreflect.ValueOfBool(stdio.TTY))
	if !stdio.TTY {
		stdioMessage.Set(types.stdin, protoreflect.ValueOfBool(stdio.Stdin))
		stdioMessage.Set(types.stdout, protoreflect.ValueOfBool(stdio.Stdout))
		stdioMessage.Set(types.stderr, protoreflect.ValueOfBool(stdio.Stderr))
	}
	request.Set(types.stdio, protoreflect.ValueOfMessage(stdioMessage))
	return request
}

func conditionKey(expressions []string) string {
	hash := sha256.New()
	var length [4]byte
	for _, expression := range expressions {
		binary.BigEndian.PutUint32(length[:], uint32(len(expression)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(expression))
	}
	return string(hash.Sum(nil))
}

type boundedCostEstimator struct{}

func (boundedCostEstimator) EstimateSize(checker.AstNode) *checker.SizeEstimate {
	size := checker.SizeEstimate{Min: 0, Max: limits.StaticCostInputSize}
	return &size
}

func (boundedCostEstimator) EstimateCallCost(string, string, *checker.AstNode, []checker.AstNode) *checker.CallEstimate {
	return nil
}

func comprehensionDepth(expression *expr.Expr, depth int) int {
	if expression == nil {
		return depth
	}
	maxDepth := depth
	visit := func(child *expr.Expr, childDepth int) {
		if got := comprehensionDepth(child, childDepth); got > maxDepth {
			maxDepth = got
		}
	}
	switch kind := expression.ExprKind.(type) {
	case *expr.Expr_SelectExpr:
		visit(kind.SelectExpr.Operand, depth)
	case *expr.Expr_CallExpr:
		visit(kind.CallExpr.Target, depth)
		for _, argument := range kind.CallExpr.Args {
			visit(argument, depth)
		}
	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.Elements {
			visit(element, depth)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.Entries {
			visit(entry.GetMapKey(), depth)
			visit(entry.Value, depth)
		}
	case *expr.Expr_ComprehensionExpr:
		next := depth + 1
		if next > maxDepth {
			maxDepth = next
		}
		comprehension := kind.ComprehensionExpr
		visit(comprehension.IterRange, next)
		visit(comprehension.AccuInit, next)
		visit(comprehension.LoopCondition, next)
		visit(comprehension.LoopStep, next)
		visit(comprehension.Result, next)
	}
	return maxDepth
}
