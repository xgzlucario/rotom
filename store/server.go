package store

import (
	"fmt"

	"github.com/panjf2000/gnet/v2"
)

type RotomEngine struct {
	gnet.BuiltinEventEngine
}

func (RotomEngine) OnTraffic(conn gnet.Conn) (action gnet.Action) {
	buf := make([]byte, 1024)
	size, err := conn.Read(buf)
	if size > 0 && err == nil {
		conn.Write([]byte("pong"))
	}
	return
}

// Listen
func (db *Store) Listen() {
	addr := fmt.Sprintf("tcp://%s:%d", db.ListenIP, db.ListenPort)

	if db.Logger != nil {
		db.Logger.Info(fmt.Sprintf("listening on %s...", addr))
	}

	err := gnet.Run(&RotomEngine{}, addr, gnet.WithMulticore(true))
	if err != nil {
		panic(err)
	}
}
