package outbound

import (
	"context"
	"os"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// getUoTConnection served sing-box UDP-over-TCP magic destinations through
// sing's GPL uot package. This fork drops that server path; every destination
// takes the normal outbound route.
func (h *Handler) getUoTConnection(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	return nil, os.ErrInvalid
}
