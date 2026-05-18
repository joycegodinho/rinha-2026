package handler

import "github.com/valyala/fasthttp"

func ReadyHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
}
