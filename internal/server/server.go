// Package server wires the event loop, client connections, command
// execution and persistence together.
package server

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xgzlucario/rotom/internal/ae"
	"github.com/xgzlucario/rotom/internal/aof"
	"github.com/xgzlucario/rotom/internal/command"
	"github.com/xgzlucario/rotom/internal/config"
	"github.com/xgzlucario/rotom/internal/db"
	"github.com/xgzlucario/rotom/internal/net"
	"github.com/xgzlucario/rotom/internal/resp"
)

const (
	KB = 1024
	MB = 1024 * KB

	// readChunk is the minimal free space ensured before each read.
	readChunk = 16 * KB
	// MaxQueryDataLen bounds the unprocessed input of a client.
	MaxQueryDataLen = 128 * MB
	// replyBufSize is the initial and idle size of a client reply buffer.
	replyBufSize = 16 * KB
	// maxAcceptsPerCall bounds accepts per readable event on the listener.
	maxAcceptsPerCall = 1000

	cronExpireMs  = 100
	cronAofMs     = 1000
	cronRewriteMs = 100
)

type Server struct {
	cfg *config.Config
	db  *db.DB
	aof *aof.AOF

	loop    *ae.Loop
	fd      int
	clients map[int]*Client
	// pending holds clients with replies to write in beforeSleep.
	pending []*Client
}

// New creates a server and loads its data (the AOF if enabled, else the RDB file).
func New(cfg *config.Config) (*Server, error) {
	s := &Server{
		cfg:     cfg,
		db:      db.New(),
		fd:      -1,
		clients: make(map[int]*Client),
	}
	if err := s.loadData(); err != nil {
		return nil, err
	}
	return s, nil
}

// loadData follows redis: when the AOF is enabled it is the source of truth,
// otherwise the RDB file is loaded. Loading both would apply the AOF on top
// of a snapshot that already contains its effects.
func (s *Server) loadData() error {
	if !s.cfg.AOF.AppendOnly {
		if !s.cfg.RDB.Save {
			return nil
		}
		log.Debug().Msg("loading rdb file...")
		dict, err := db.LoadRDB(s.cfg.RDB.DBFileName)
		if err != nil {
			return fmt.Errorf("load rdb: %w", err)
		}
		s.db.Dict = dict
		return nil
	}

	policy, err := aof.ParseFsyncPolicy(s.cfg.AOF.AppendFsync)
	if err != nil {
		return err
	}
	a, err := aof.Open(s.cfg.AOF.AppendDirName, s.cfg.AOF.AppendFileName, policy)
	if err != nil {
		return err
	}
	log.Debug().Msg("loading aof...")
	ctx := s.newContext(resp.NewWriter(KB))
	loadBase := func(path string) error {
		dict, err := db.LoadRDB(path)
		if err == nil {
			s.db.Dict = dict
		}
		return err
	}
	replay := func(argv [][]byte) {
		if cmd := command.Lookup(argv[0]); cmd != nil {
			command.Execute(cmd, ctx, argv[1:])
			ctx.W.Reset()
		}
	}
	if err := a.Load(loadBase, replay); err != nil {
		_ = a.Close()
		return err
	}
	s.aof = a
	return nil
}

// rewriteAOF starts a background AOF rewrite. Encoding the snapshot blocks
// the event loop; writing it to disk does not.
func (s *Server) rewriteAOF() error {
	if s.aof.Rewriting() {
		return aof.ErrRewriteInProgress
	}
	start := time.Now()
	snapshot := db.EncodeRDB(s.db.Dict)
	if err := s.aof.StartRewrite(snapshot); err != nil {
		return err
	}
	log.Info().Msgf("aof rewrite started: %d keys, %d bytes snapshot encoded in %v",
		s.db.Len(), len(snapshot), time.Since(start))
	return nil
}

// cronRewriteAof finishes a background rewrite, and starts one when the AOF
// has grown enough, like redis' auto-aof-rewrite-percentage/min-size.
func (s *Server) cronRewriteAof(*ae.Loop, int, any) int64 {
	if done, err := s.aof.PollRewrite(); err != nil {
		log.Error().Msgf("aof rewrite failed: %v", err)
	} else if done {
		log.Info().Msgf("aof rewrite finished, size %d bytes", s.aof.Size())
	}

	pct := int64(s.cfg.AOF.AutoRewritePercentage)
	size := s.aof.Size()
	if pct > 0 && !s.aof.Rewriting() && size >= int64(s.cfg.AOF.AutoRewriteMinSize) {
		base := max(s.aof.SizeAfterRewrite(), 1)
		if growth := (size - base) * 100 / base; growth >= pct {
			log.Info().Msgf("starting automatic aof rewrite: %d%% growth", growth)
			if err := s.rewriteAOF(); err != nil {
				log.Error().Msgf("aof rewrite: %v", err)
			}
		}
	}
	return cronRewriteMs
}

// Listen opens the listening socket and registers the event handlers.
func (s *Server) Listen() (err error) {
	if s.loop, err = ae.New(); err != nil {
		return err
	}
	if s.fd, err = net.TcpServer(s.cfg.TCP.Port); err != nil {
		return err
	}
	if err = s.loop.AddFileEvent(s.fd, ae.Readable, s.acceptHandler, nil); err != nil {
		return err
	}
	s.loop.SetBeforeSleep(s.beforeSleep)
	s.loop.AddTimeEvent(cronExpireMs, s.cronExpire, nil)
	if s.aof != nil {
		s.loop.AddTimeEvent(cronAofMs, s.cronSyncAof, nil)
		s.loop.AddTimeEvent(cronRewriteMs, s.cronRewriteAof, nil)
	}
	return nil
}

// Run serves until Stop is called. It must run on a single goroutine.
func (s *Server) Run() { s.loop.Main() }

// Close releases sockets and flushes the AOF.
func (s *Server) Close() error {
	for _, c := range s.clients {
		s.freeClient(c)
	}
	if s.fd >= 0 {
		_ = net.Close(s.fd)
	}
	if s.loop != nil {
		_ = s.loop.Close()
	}
	if s.aof != nil {
		return s.aof.Close()
	}
	return nil
}

func (s *Server) newContext(w *resp.Writer) *command.Context {
	ctx := &command.Context{W: w, DB: s.db, Config: s.cfg}
	if s.cfg.AOF.AppendOnly {
		ctx.RewriteAOF = s.rewriteAOF
	}
	return ctx
}

// processCommand runs one command with ctx, appending the reply to ctx.W.
// raw is the original RESP encoding of argv, used for the AOF.
func (s *Server) processCommand(ctx *command.Context, argv [][]byte, raw []byte) {
	cmd := command.Lookup(argv[0])
	if cmd == nil {
		ctx.W.WriteError(command.UnknownCommandError(argv[0]))
		return
	}
	res := command.Execute(cmd, ctx, argv[1:])
	if !res.Persist || s.aof == nil {
		return
	}
	if res.Argv != nil {
		s.aof.Append(res.Argv)
	} else {
		s.aof.AppendRaw(raw)
	}
}

// beforeSleep runs before the event loop polls. Commands executed in this
// iteration are written to the AOF first, then their replies are sent, so a
// client never sees a reply for a write that is not in the AOF yet.
func (s *Server) beforeSleep(*ae.Loop) {
	if s.aof != nil {
		if err := s.aof.Write(); err != nil {
			log.Error().Msgf("write aof error: %v", err)
		}
	}
	s.handlePendingWrites()
}

func (s *Server) cronSyncAof(*ae.Loop, int, any) int64 {
	if s.aof.Policy() == aof.FsyncEverySec {
		if err := s.aof.Sync(); err != nil {
			log.Error().Msgf("sync aof error: %v", err)
		}
	}
	return cronAofMs
}

func (s *Server) cronExpire(*ae.Loop, int, any) int64 {
	// redis spends at most 25% of each cron period on active expire.
	s.db.ActiveExpire(cronExpireMs * time.Millisecond / 4)
	return cronExpireMs
}
