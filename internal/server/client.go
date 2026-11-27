package server

import (
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
	"github.com/xgzlucario/rotom/internal/ae"
	"github.com/xgzlucario/rotom/internal/net"
	"github.com/xgzlucario/rotom/internal/resp"
)

type Client struct {
	fd int

	// queryBuf[qpos:] is received but not yet processed input.
	queryBuf []byte
	qpos     int
	argv     [][]byte

	// reply[sentlen:] is pending output.
	reply   *resp.Writer
	sentlen int

	// closeAfterReply closes the connection once pending output is flushed,
	// e.g. after a protocol error.
	closeAfterReply bool

	// pendingWrite is set while the client is queued in Server.pending.
	pendingWrite bool
}

func (c *Client) hasPendingReply() bool { return c.sentlen < c.reply.Len() }

func (s *Server) acceptHandler(loop *ae.Loop, fd int, _ any) {
	for range maxAcceptsPerCall {
		cfd, err := net.Accept(fd)
		if err != nil {
			if !net.IsTemporary(err) {
				log.Error().Msgf("accept err: %v", err)
			}
			return
		}
		c := &Client{
			fd:       cfd,
			reply:    resp.NewWriter(replyBufSize),
			queryBuf: make([]byte, 0, readChunk),
			argv:     make([][]byte, 0, 8),
		}
		if err := loop.AddFileEvent(cfd, ae.Readable, s.readQueryFromClient, c); err != nil {
			log.Error().Msgf("add client fd %d err: %v", cfd, err)
			_ = net.Close(cfd)
			continue
		}
		s.clients[cfd] = c
		log.Debug().Msgf("accept new client fd: %d", cfd)
	}
}

func (s *Server) freeClient(c *Client) {
	if s.clients[c.fd] != c {
		return
	}
	delete(s.clients, c.fd)
	s.loop.DelFileEvent(c.fd, ae.Readable|ae.Writable)
	_ = net.Close(c.fd)
	log.Debug().Msgf("free client %d", c.fd)
}

// makeRoom ensures at least readChunk bytes of free space in queryBuf.
func (c *Client) makeRoom() {
	if cap(c.queryBuf)-len(c.queryBuf) >= readChunk {
		return
	}
	// move unprocessed data to the front first.
	if c.qpos > 0 {
		n := copy(c.queryBuf, c.queryBuf[c.qpos:])
		c.queryBuf = c.queryBuf[:n]
		c.qpos = 0
	}
	if cap(c.queryBuf)-len(c.queryBuf) < readChunk {
		newBuf := make([]byte, len(c.queryBuf), max(2*cap(c.queryBuf), len(c.queryBuf)+readChunk))
		copy(newBuf, c.queryBuf)
		c.queryBuf = newBuf
	}
}

func (s *Server) readQueryFromClient(_ *ae.Loop, fd int, extra any) {
	c := extra.(*Client)
	c.makeRoom()

	start := len(c.queryBuf)
	n, err := net.Read(fd, c.queryBuf[start:cap(c.queryBuf)])
	if err != nil {
		if net.IsTemporary(err) {
			return
		}
		log.Debug().Msgf("client %d read err: %v", fd, err)
		s.freeClient(c)
		return
	}
	if n == 0 {
		// connection closed by peer
		s.freeClient(c)
		return
	}
	c.queryBuf = c.queryBuf[:start+n]

	if len(c.queryBuf)-c.qpos > MaxQueryDataLen {
		log.Warn().Msgf("client %d query buffer exceeds %d bytes, closing", fd, MaxQueryDataLen)
		s.freeClient(c)
		return
	}
	s.processInputBuffer(c)
}

func (s *Server) processInputBuffer(c *Client) {
	for c.qpos < len(c.queryBuf) && !c.closeAfterReply {
		buf := c.queryBuf[c.qpos:]
		complete, args, _, left, err := redcon.ReadNextCommand(buf, c.argv[:0])
		if err != nil {
			c.reply.WriteError("ERR Protocol error: " + err.Error())
			c.closeAfterReply = true
			break
		}
		if !complete {
			break
		}
		c.argv = args[:0] // keep the grown buffer for next time
		n := len(buf) - len(left)
		c.qpos += n
		if len(args) > 0 {
			s.processCommand(c.reply, args, buf[:n])
		}
	}

	if c.qpos == len(c.queryBuf) {
		c.queryBuf = c.queryBuf[:0]
		c.qpos = 0
	}

	// Queue the client: its reply is written directly in beforeSleep, which
	// avoids registering (and later removing) a writable event per request.
	if c.hasPendingReply() && !c.pendingWrite && s.loop.FileEvents(c.fd)&ae.Writable == 0 {
		c.pendingWrite = true
		s.pending = append(s.pending, c)
	}
}

// handlePendingWrites writes queued replies directly. Clients whose reply
// does not fit into the socket buffer get a writable event instead.
func (s *Server) handlePendingWrites() {
	for i, c := range s.pending {
		s.pending[i] = nil
		c.pendingWrite = false
		if s.clients[c.fd] != c {
			continue // freed in the meantime
		}
		if !s.writeToClient(c) || !c.hasPendingReply() {
			continue
		}
		if err := s.loop.AddFileEvent(c.fd, ae.Writable, s.sendReplyToClient, c); err != nil {
			log.Error().Msgf("add write event err: %v", err)
			s.freeClient(c)
		}
	}
	s.pending = s.pending[:0]
}

// writeToClient writes as much pending output as the socket accepts.
// It returns false if the client was freed.
func (s *Server) writeToClient(c *Client) bool {
	for c.hasPendingReply() {
		n, err := net.Write(c.fd, c.reply.Bytes()[c.sentlen:])
		if err != nil {
			if net.IsTemporary(err) {
				return true // socket buffer full
			}
			log.Debug().Msgf("client %d write err: %v", c.fd, err)
			s.freeClient(c)
			return false
		}
		c.sentlen += n
	}
	c.reply.Reset()
	c.reply.Shrink(replyBufSize)
	c.sentlen = 0
	if c.closeAfterReply {
		s.freeClient(c)
		return false
	}
	return true
}

// sendReplyToClient is the writable handler for replies that did not fit
// into the socket buffer at once.
func (s *Server) sendReplyToClient(loop *ae.Loop, fd int, extra any) {
	c := extra.(*Client)
	if s.writeToClient(c) && !c.hasPendingReply() {
		loop.DelFileEvent(fd, ae.Writable)
	}
}
