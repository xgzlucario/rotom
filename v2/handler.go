package main

var (
	RespOK   = []byte("OK")
	RespPong = []byte("PONG")
)

var HSETs = map[string]map[string]string{}

func pingCommand(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: TypeString, str: RespPong}
	}
	return Value{typ: TypeString, str: args[0].bulk}
}

func setCommand(args []Value) Value {
	key := args[0].bulk
	value := args[1].bulk

	server.db.strs.Set(b2s(key), value)

	return Value{typ: TypeString, str: RespOK}
}

func getCommand(args []Value) Value {
	key := args[0].bulk

	value, _, ok := server.db.strs.Get(b2s(key))
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: value}
}

func hsetCommand(args []Value) Value {
	hash := b2s(args[0].bulk)
	key := b2s(args[1].bulk)
	value := b2s(args[2].bulk)

	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value

	return Value{typ: TypeString, str: RespOK}
}

func hgetCommand(args []Value) Value {
	hash := args[0].bulk
	key := args[1].bulk

	value, ok := HSETs[b2s(hash)][b2s(key)]
	if !ok {
		return Value{typ: TypeNull}
	}

	return Value{typ: TypeBulk, bulk: []byte(value)}
}

func hgetallCommand(args []Value) Value {
	hash := args[0].bulk

	value, ok := HSETs[b2s(hash)]
	if !ok {
		return Value{typ: TypeNull}
	}

	var values []Value
	for k, v := range value {
		values = append(values, Value{typ: TypeBulk, bulk: []byte(k)})
		values = append(values, Value{typ: TypeBulk, bulk: []byte(v)})
	}

	return Value{typ: TypeArray, array: values}
}
