# ADR-0001: G0 foundation boundaries

- Status: ACCEPTED FOR G0 PHASE A
- Date: 2026-08-23

## Context

Issue #1 requests the smallest reproducible C++20 Gateway foundation while the Contracts package
publication and the Projection M6 candidate gate remain pending. The M6 integration contract
explicitly forbids a new Gateway-to-Projection host abstraction and later runtime behavior in this
phase.

## Decision

Phase A contains only:

1. a typed, synchronously validated configuration for Binance venue, Spot/USD-M perpetual market,
   one opaque non-empty symbol (without NUL, control, or ASCII-whitespace bytes), a future gRPC
   listen endpoint, and a nonzero finite queue-capacity value; and
2. a synchronous foundation lifecycle with explicit constructed/running/stopped states.

The foundation creates no threads, sockets, queues, clocks, callbacks, or asynchronous work. The
daemon validates its flags and demonstrates the lifecycle, then exits. The endpoint is not bound.
G0 does not invent symbol grammar, normalize case, or assert exchange membership; later transport
code owns official lowercase stream mapping and exchangeInfo validation.

An opt-in link smoke discovers the Contracts message-only package, the separate Contracts gRPC
package, and Projection Core/ProtoAdapter using their exported target names. It links those targets
in one final executable and includes their public types. It is disabled by default and requires an
explicit staged prefix; no unassigned RREV, SHA, copied proto, or floating dependency is allowed.

## Consequences

G0 is independently buildable without third-party dependencies. Later transport and Host work can
attach to a narrow lifecycle/configuration seam, but no Phase A API claims ownership of Projection
semantics. Upstream package publication and official protocol evidence remain explicit gates.
