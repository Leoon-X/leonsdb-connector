# LeonsDB connector and client SDK

The open-source client side of [LeonsDB](https://leonsdb.com), the database
built for Minecraft networks:

- **Connector** (Java 21): the library and the Paper plugin (which also runs
  on Folia) that let Minecraft plugins keep player and global data in
  LeonsDB with plain Java classes.
  See [docs/plugin-developers.md](docs/plugin-developers.md).
- **Velocity plugin**: tells LeonsDB which server a player is about to join,
  so server switches wait less. It holds no player data.
- **Client SDK**: the protocol definitions (`proto/`, Protocol Buffers and
  gRPC), the generated Go code, and a Go client for the LeonsDB wire
  protocol ([docs/wire-protocol.md](docs/wire-protocol.md)). Use them to
  talk to LeonsDB from any language.

The LeonsDB server itself is not part of this repository. It is free to use
and is distributed as compiled builds from [leonsdb.com](https://leonsdb.com).

## Modules

| Path | What it is |
|---|---|
| `api/` | The API plugins compile against (`@PlayerData`, `LeonsDB`, …) |
| `core/` | Networking, caching and write merging, platform-independent |
| `paper/` | The Paper plugin, which also runs on Folia |
| `velocity/` | The Velocity plugin (switch hints) |
| `protocol/` | Java classes generated from `proto/` |
| `example/` | An example plugin |
| `proto/` | Protocol definitions, generated Go code, Go wire client (`proto/wireclient`) |

## Building

```
./gradlew build
```

The contract tests in `core` run against a real LeonsDB server when given its
binary: `./gradlew test -PleonsdbServer=/path/to/leonsdb`.

The Go code in `proto/gen` is generated: after changing a `.proto` file, run
`scripts/gen-proto.sh` (needs `protoc`, `protoc-gen-go` and
`protoc-gen-go-grpc`).

## License

Apache License 2.0, see [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright (c) 2026 Leoon-X.

LeonsDB™ is a trademark of Leoon-X. Minecraft is a trademark of Mojang
Synergies AB; this project is not an official Minecraft product and is not
approved by or associated with Mojang or Microsoft.
