package gobayeux

import "context"

type transport interface {
	request(context.Context, []Message) ([]Message, error)
	transportType() string
}
