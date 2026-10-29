// CloudProbe local compatibility patch: keep legacy text and expose response
// rejection fields so callers need not guess execution state from error strings.
package wopan

type ResponseError struct {
	HTTPStatus int
	RspCode    string
	Detail     string
}

func (e *ResponseError) Error() string { return e.Detail }
