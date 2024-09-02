package main

var Handlers = map[string]func([]token) token {
  "PING": ping,
  "ECHO": echo,
}

func echo(args []token) token {
  if len(args) == 0 {
    return token{typ: string(STRING), val: ""}
  }

  return token{typ: string(STRING), val: args[0].bulk }
}

func ping(args []token) token {
  if len(args) == 0 {
    return token{typ: string(STRING), val: "PONG"}
  }
  
  return token{typ: string(STRING), val: args[0].bulk}
}
