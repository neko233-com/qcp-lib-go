# qcp-lib-go

Go binding for QCP protocol.

## Installation

```bash
go get github.com/neko233-com/qcp-lib-go
```

## Usage

```go
package main

import (
    "github.com/neko233-com/qcp-lib-go/qcp"
)

func main() {
    // Create QCP client
    client := qcp.NewClient()
    
    // Connect to server
    client.Connect("127.0.0.1:9000")
    
    // Send data
    client.Send([]byte("hello"))
    
    // Receive data
    buf := make([]byte, 1024)
    n, _ := client.Recv(buf)
    
    // Close
    client.Close()
}
```

## Features

- Pure Go implementation (no CGo)
- Goroutine-safe
- Configurable congestion control
- Built-in FEC support

## License

MIT License
