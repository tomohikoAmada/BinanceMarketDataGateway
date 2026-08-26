# Gateway G1 exact candidate dependency integration

G1 is a candidate-only consumer proof. It does not claim a formal Contracts or Projection
release, finalized PREV identities, deployment acceptance, or any G2/runtime behavior.

The proof consumes one exact candidate lineage:

| Artifact | Source | Conan RREV |
|---|---|---|
| Contracts message | `9c34199f42467b92a4380c6e88617dde0e54ae13` | `53713fb82c27cf6ea0395f6b8a853006` |
| Contracts gRPC | `9c34199f42467b92a4380c6e88617dde0e54ae13` | `c3661aa735e1eb6450c1a93f635dbc20` |
| Projection | `87d05af38d7173ca2d5cae13e7592c38495ec895` | `58ab66309450bdc2035b3e5935220348` |

The observed Mac ARM64 Release/static package identities used for this proof are recorded below.
The graph verifier derives and checks each package ID and PREV from the actual Conan graph node;
it also checks these documented values:

| Package | Package ID | PREV |
|---|---|---|
| Contracts message | `a1a286da6ca09b590d78bcb14d8250c025131c29` | `244330f80db8b6aaed1adbe6f90f825e` |
| Contracts gRPC | `3acd5bb035736877e698472136645086258451a7` | `80a1c50b6d77fae2d6bcc2fb895ba7ae` |
| Projection | `cb502a97da45967059ab2838b16d0fa48417c4a7` | `1af4eac37f3e73001b6109f095fa1a54` |

Run the focused proof with the isolated Conan cache containing those exact candidate packages:

```sh
BMD_GATEWAY_CONAN=/path/to/conan scripts/g1-candidate-proof.sh
```

The proof clears external CMake package-path hints, installs the graph with `--build=never`, and
lets Conan's `CMakeToolchain` consume the package `cpp_info.builddirs` metadata. Projection,
Contracts, and Contracts gRPC therefore resolve their installed canonical configs under
`lib/cmake/<PackageName>` while retaining `cmake_find_mode=none`.

The graph verifier requires the message package RREV to appear only as
`binance-market-data-contracts-cpp/0.1.0#53713fb82c27cf6ea0395f6b8a853006`. The CMake smoke
discovers and links `Contracts::Protobuf`, `Contracts::Grpc`, `Projection::Core`, and
`Projection::ProtoAdapter` in one executable. It uses public message, service, Core, and adapter
symbols only; it adds no networking, queues, bootstrap, sequencing, order-book logic, Recorder
dependency, or copied `.proto` files.
