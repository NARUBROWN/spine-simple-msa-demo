package order

import (
	"context"
	spinectx "github.com/NARUBROWN/spine/pkg/spine"
)

// Only the execution context is needed when these handlers are called directly.
type testHandlerContext struct {
	spinectx.Ctx
	execution spinectx.ExecutionContextView
}

func (c testHandlerContext) ExecutionContext() spinectx.ExecutionContextView { return c.execution }

type testExecutionContext struct {
	spinectx.ExecutionContextView
	ctx context.Context
}

func (c testExecutionContext) Context() context.Context { return c.ctx }
func handlerContext(ctx context.Context) spinectx.Ctx {
	return testHandlerContext{execution: testExecutionContext{ctx: ctx}}
}
