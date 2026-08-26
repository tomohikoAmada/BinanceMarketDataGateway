# Gateway G1 exact candidate dependency integration

G1 is a candidate-only consumer proof. It does not claim a formal Contracts or Projection
release, finalized PREV identities, deployment acceptance, or any G2/runtime behavior.

The proof consumes one exact candidate lineage:

| Artifact | Source | Conan RREV |
|---|---|---|
| Contracts message | `9c34199f42467b92a4380c6e88617dde0e54ae13` | `53713fb82c27cf6ea0395f6b8a853006` |
| Contracts gRPC | `9c34199f42467b92a4380c6e88617dde0e54ae13` | `c3661aa735e1eb6450c1a93f635dbc20` |
| Projection | `54c359c15d02b444020c3e210d0ff9e6a4b2a1a9` | `5c8ee9626b652fad1075fa44e480182b` |

The observed Mac ARM64 Release/static package identities used for this proof are:

| Package | Package ID | PREV |
|---|---|---|
| Contracts message | `a1a286da6ca09b590d78bcb14d8250c025131c29` | `244330f80db8b6aaed1adbe6f90f825e` |
| Contracts gRPC | `3acd5bb035736877e698472136645086258451a7` | `633160ac0940770b666ceb7558419722` |
| Projection | `cb502a97da45967059ab2838b16d0fa48417c4a7` | `80278337c58fdc8ba92ee4622d9a2e12` |

Run the focused proof with the isolated Conan cache containing those exact candidate packages:

```sh
BMD_GATEWAY_CONAN=/path/to/conan scripts/g1-candidate-proof.sh
```

The graph verifier requires the message package RREV to appear only as
`binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006`. The CMake smoke
discovers and links `Contracts::Protobuf`, `Contracts::Grpc`, `Projection::Core`, and
`Projection::ProtoAdapter` in one executable. It uses public message, service, Core, and adapter
symbols only; it adds no networking, queues, bootstrap, sequencing, order-book logic, Recorder
dependency, or copied `.proto` files.
