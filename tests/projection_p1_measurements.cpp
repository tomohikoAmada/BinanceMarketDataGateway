// Offline measurement driver: reuse G2's production graph and external-I/O
// seam.
#include "four_product_test_support.hpp"
#include <barrier>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <fstream>
#include <iostream>
#include <sys/resource.h>
#include <thread>

namespace {
namespace fixture = binance_market_data::gateway::production::four_product_test;
namespace production = binance_market_data::gateway::production;
namespace support = production::test_support;
namespace g3 = binance_market_data::gateway::g3;
namespace g4 = binance_market_data::gateway::g4;
namespace g7 = binance_market_data::gateway::g7;
namespace g9 = binance_market_data::gateway::g9;
namespace g11 = binance_market_data::gateway::g11;
namespace wire = binance_market_data::gateway::v1;
namespace common = binance_market_data::common::v1;
using Json = nlohmann::json;
using Clock = std::chrono::steady_clock;
constexpr std::uint64_t first = 135U;
constexpr std::size_t warmup = 32U;

std::uint64_t now() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          Clock::now().time_since_epoch())
          .count());
}
std::uint64_t cpu_us(const rusage &r) {
  return static_cast<std::uint64_t>(r.ru_utime.tv_sec + r.ru_stime.tv_sec) *
             1000000ULL +
         static_cast<std::uint64_t>(r.ru_utime.tv_usec + r.ru_stime.tv_usec);
}
Json stats(std::vector<std::uint64_t> values) {
  fixture::check(!values.empty());
  std::sort(values.begin(), values.end());
  const auto q = [&](std::size_t percent) {
    return values[(values.size() * percent + 99U) / 100U - 1U];
  };
  std::uint64_t sum = 0;
  for (const auto v : values) {
    sum += v;
  }
  return {{"n", values.size()},
          {"mean_ns", sum / values.size()},
          {"p50_ns", q(50U)},
          {"p99_ns", q(99U)},
          {"max_ns", values.back()}};
}

std::string frame(const g11::MarketKey &key, std::uint64_t id,
                  std::size_t batch, std::size_t depth) {
  Json bids = Json::array();
  for (std::size_t j = 0; j < batch; ++j) {
    bids.push_back({std::to_string(10000U - j % depth), id % 2U ? "2" : "3"});
  }
  Json value{{"e", "depthUpdate"},
             {"E", 1700000000000ULL + id},
             {"s", key.symbol},
             {"U", id},
             {"u", id},
             {"b", bids},
             {"a", Json::array()}};
  if (key.market == common::MARKET_SPOT) {
    return Json{{"stream",
                 g4::make_spot_transport_routes(key.symbol)->diff_depth_stream},
                {"data", value}}
        .dump();
  }
  value["U"] = id - 1U;
  value["pu"] = id - 1U;
  value["T"] = 1700000000001ULL + id;
  return value.dump();
}

support::market::DepthUpdate parse(const g11::MarketKey &key,
                                   const std::string &text) {
  if (key.market == common::MARKET_SPOT) {
    auto value = g4::parse_combined_event_frame(text, g4::sample_real_clock(),
                                                key.symbol + "-g1", key.symbol);
    fixture::check(std::holds_alternative<g4::NormalizedMarketEvent>(value));
    return std::get<support::market::DepthUpdate>(
        std::get<g4::NormalizedMarketEvent>(std::move(value)));
  }
  auto value = g11::parse_usdm_depth_frame(text, g4::sample_real_clock(),
                                           key.symbol + "-g1", key.symbol);
  fixture::check(std::holds_alternative<support::market::DepthUpdate>(value));
  return std::get<support::market::DepthUpdate>(std::move(value));
}

struct Client final {
  grpc::ClientContext book_context;
  grpc::ClientContext event_context;
  std::unique_ptr<grpc::ClientReader<wire::OrderBookStreamItem>> book;
  std::unique_ptr<grpc::ClientReader<wire::GatewayEventEnvelope>> events;
  std::vector<std::uint64_t> book_times;
  std::vector<std::uint64_t> event_times;
  std::atomic<std::size_t> received{0U};
  std::atomic<bool> valid{true};
  std::jthread book_thread;
  std::jthread event_thread;
  Client(wire::BinanceMarketDataGatewayService::Stub &stub,
         const g11::MarketKey &key, std::size_t count,
         std::size_t expected_depth, std::chrono::seconds timeout)
      : book_times(count), event_times(count) {
    book_context.set_deadline(std::chrono::system_clock::now() + timeout);
    event_context.set_deadline(std::chrono::system_clock::now() + timeout);
    wire::OrderBookSubscriptionRequest br;
    br.set_request_id("p1-book");
    br.set_schema_version(g7::kOrderBookRequestSchema);
    br.set_venue(key.venue);
    br.set_market(key.market);
    br.set_symbol(key.symbol);
    br.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
    br.add_supported_snapshot_schema_versions(g7::kSnapshotSchema);
    br.add_supported_update_schema_versions(g7::kUpdateSchema);
    book = stub.SubscribeOrderBook(&book_context, br);
    wire::OrderBookStreamItem initial;
    fixture::check(book->Read(&initial) && initial.has_subscription_accepted());
    fixture::check(book->Read(&initial) && initial.has_snapshot());
    fixture::check(initial.snapshot().symbol() == key.symbol &&
                   initial.snapshot().market() == key.market);
    fixture::check(
        initial.snapshot().bids_size() == static_cast<int>(expected_depth) &&
        initial.snapshot().asks_size() == static_cast<int>(expected_depth));
    wire::EventSubscriptionRequest er;
    er.set_request_id("p1-event");
    er.set_schema_version(g9::kEventRequestSchema);
    auto *selector = er.add_selectors();
    selector->set_venue(key.venue);
    selector->set_market(key.market);
    selector->set_symbol(key.symbol);
    selector->set_stream(common::STREAM_DIFF_DEPTH);
    er.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
    er.add_supported_payload_schema_versions(g9::kDepthEventSchema);
    events = stub.SubscribeEvents(&event_context, er);
    wire::GatewayEventEnvelope initial_event;
    fixture::check(events->Read(&initial_event) &&
                   initial_event.has_subscription_accepted());
    const auto consume = [this, key, count](auto &reader, auto &times,
                                            auto item) {
      for (std::size_t i = 0; i < warmup + count; ++i) {
        if (!reader->Read(&item) || !item.has_depth_update() ||
            item.depth_update().final_update_id() != first - warmup + i ||
            item.depth_update().metadata().symbol() != key.symbol ||
            item.depth_update().metadata().market() != key.market) {
          valid.store(false);
          return;
        }
        if (i >= warmup) {
          times[i - warmup] = now();
        }
        received.fetch_add(1U);
      }
    };
    book_thread = std::jthread{[&, consume] {
      consume(book, book_times, wire::OrderBookStreamItem{});
    }};
    event_thread = std::jthread{[&, consume] {
      consume(events, event_times, wire::GatewayEventEnvelope{});
    }};
  }
  ~Client() {
    book_context.TryCancel();
    event_context.TryCancel();
  }
};

void wait_clients(const std::vector<std::unique_ptr<Client>> &clients,
                  std::size_t count) {
  const auto deadline = Clock::now() + std::chrono::seconds{10};
  for (const auto &client : clients) {
    while (client->received.load() != count) {
      fixture::check(client->valid.load() && Clock::now() < deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  }
}

int run(int argc, char **argv) {
  fixture::check(argc == 6);
  const auto depth = std::stoul(argv[1]);
  const auto batch = std::stoul(argv[2]);
  const auto count = std::stoul(argv[3]);
  const auto interval = std::stoul(argv[4]);
  fixture::check(depth > 0U && depth <= 10000U && batch > 0U &&
                 batch <= depth && count > 0U && count <= 16000U &&
                 interval <= 1000000U);
  auto config = fixture::configure_file(BMD_GATEWAY_FOUR_PRODUCT_CONFIG);
  const auto keys = config.config.market_keys;
  production::GatewayOptions options;
  options.allow_ephemeral_listen_for_testing = true;
  production::ProductionGateway gateway{std::move(config.specs),
                                        g4::sample_real_clock, "p1-measurement",
                                        "127.0.0.1:0", options};
  fixture::check(gateway.start().code == production::StartCode::Serving);
  auto stub =
      wire::BinanceMarketDataGatewayService::NewStub(grpc::CreateChannel(
          "127.0.0.1:" + std::to_string(gateway.observe().selected_port),
          grpc::InsecureChannelCredentials()));
  std::vector<std::unique_ptr<Client>> clients;
  std::array<std::vector<std::string>, 4> frames;
  for (std::size_t p = 0; p < 4U; ++p) {
    auto *product = gateway.products_for_testing().find(keys[p]);
    auto seed = fixture::depth(keys[p], 1U, 102U);
    seed.clear_bids();
    seed.clear_asks();
    auto *remove_bid = seed.add_bids();
    remove_bid->set_price(fixture::sample(keys[p]).bid);
    remove_bid->set_quantity("0");
    auto *remove_ask = seed.add_asks();
    remove_ask->set_price(fixture::sample(keys[p]).ask);
    remove_ask->set_quantity("0");
    for (std::size_t j = 0; j < depth; ++j) {
      auto *bid = seed.add_bids();
      bid->set_price(std::to_string(10000U - j));
      bid->set_quantity("1");
      auto *ask = seed.add_asks();
      ask->set_price(std::to_string(20000U + j));
      ask->set_quantity("1");
    }
    fixture::check(product->runtime().submit_depth_update(seed, {1U}) ==
                   g3::AdmissionResult::Accepted);
    fixture::check(product->runtime().observe().last_update_id == 102U);
    clients.push_back(std::make_unique<Client>(
        *stub, keys[p], count, depth,
        std::chrono::seconds{30 + count * interval / 1000000U}));
    for (std::size_t i = 0; i < warmup + count; ++i) {
      frames[p].push_back(frame(keys[p], first - warmup + i, batch, depth));
    }
  }
  std::array<std::vector<std::uint64_t>, 4> starts;
  for (auto &v : starts) {
    v.resize(count);
  }
  const auto submit = [&](std::size_t p, std::size_t i) {
    auto *product = gateway.products_for_testing().find(keys[p]);
    const auto t0 = now();
    auto update = parse(keys[p], frames[p][i]);
    if (i >= warmup) {
      starts[p][i - warmup] = t0;
    }
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
    // External-I/O fixture is the single trace producer, as the transport is in
    // production.
    auto &buffer = const_cast<
        binance_market_data::gateway::performance::ProductTraceBuffer &>(
        product->performance_baseline());
    const auto trace =
        i >= warmup ? buffer.begin_trace(t0, now())
                    : binance_market_data::gateway::performance::TraceToken{};
#endif
    fixture::check(
        product->event_publication().publish(
            std::make_shared<const g4::NormalizedMarketEvent>(update), 1U
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
            ,
            trace
#endif
            ) == g9::EventPublishResult::Published);
    fixture::check(product->runtime().submit_depth_update(
                       std::move(update), {1U}
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                       ,
                       trace
#endif
                       ) == g3::AdmissionResult::Accepted);
  };
  for (std::size_t i = 0; i < warmup; ++i) {
    for (std::size_t p = 0; p < 4U; ++p) {
      submit(p, i);
    }
    wait_clients(clients, 2U * (i + 1U));
  }
  std::barrier start_gate{5};
  std::atomic<bool> producers_ok{true};
  std::vector<std::jthread> producers;
  for (std::size_t p = 0; p < 4U; ++p) {
    producers.emplace_back([&, p] {
      start_gate.arrive_and_wait();
      try {
        const auto begin = Clock::now();
        for (std::size_t i = 0; i < count; ++i) {
          std::this_thread::sleep_until(
              begin + std::chrono::microseconds{i * interval});
          submit(p, i + warmup);
        }
      } catch (...) {
        producers_ok.store(false);
      }
    });
  }
  rusage before{};
  fixture::check(getrusage(RUSAGE_SELF, &before) == 0);
  const auto began = now();
  start_gate.arrive_and_wait();
  for (auto &producer : producers) {
    producer.join();
  }
  fixture::check(producers_ok.load());
  wait_clients(clients, 2U * (warmup + count));
  const auto ended = now();
  rusage after{};
  fixture::check(getrusage(RUSAGE_SELF, &after) == 0);
  Json result{{"schema", "projection-p1-offline.v1"},
              {"depth_per_side", depth},
              {"batch_levels", batch},
              {"events_per_product", count},
              {"interval_us", interval},
              {"wall_ns", ended - began},
              {"process_cpu_us", cpu_us(after) - cpu_us(before)},
              {"process_cpu_includes",
               "four owners + producer/parser + loopback "
               "server AND clients + recovery"},
              {"products", Json::array()}};
#ifdef __APPLE__
  result["peak_rss_process_bytes"] = after.ru_maxrss;
#else
  result["peak_rss_process_bytes"] = after.ru_maxrss * 1024L;
#endif
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  result["instrumented"] = true;
#else
  result["instrumented"] = false;
#endif
  for (std::size_t p = 0; p < 4U; ++p) {
    const auto observed =
        gateway.products_for_testing().find(keys[p])->runtime().observe();
    fixture::check(observed.last_update_id == first + count - 1U &&
                   observed.state == g3::RuntimeState::Live);
    std::vector<std::uint64_t> books, events;
    for (std::size_t i = 0; i < count; ++i) {
      fixture::check(clients[p]->book_times[i] >= starts[p][i] &&
                     clients[p]->event_times[i] >= starts[p][i]);
      books.push_back(clients[p]->book_times[i] - starts[p][i]);
      events.push_back(clients[p]->event_times[i] - starts[p][i]);
    }
    result["products"].push_back(
        {{"market", common::Market_Name(keys[p].market)},
         {"symbol", keys[p].symbol},
         {"book_client_latency", stats(books)},
         {"event_client_latency", stats(events)},
         {"last_update_id", *observed.last_update_id}});
  }
  gateway.stop();
  for (const auto &client : clients) {
    client->book_thread.join();
    client->event_thread.join();
  }
  fixture::check(gateway.observe().tracked_contexts == 0U);
  for (const auto &product : gateway.observe().products) {
    fixture::check(product.runtime.owner_joined &&
                   product.recovery.active_transport_count == 0U);
  }
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
  std::ofstream traces{std::string{argv[5]} + ".traces.jsonl"};
  fixture::check(gateway.write_performance_baseline(traces));
  for (const auto &product : gateway.products_for_testing().products()) {
    const auto &buffer = product->performance_baseline();
    fixture::check(buffer.evidence_valid() && buffer.trace_count() == count);
    fixture::check(
        buffer.delivery_count(binance_market_data::gateway::performance::
                                  DeliveryKind::OrderBook) == count &&
        buffer.delivery_count(
            binance_market_data::gateway::performance::DeliveryKind::Event) ==
            count);
    for (std::size_t i = 0; i < count; ++i) {
      fixture::check(
          buffer.trace(i).t3_disposition ==
          binance_market_data::gateway::performance::T3Disposition::Applied);
    }
  }
#endif
  std::ofstream output{argv[5]};
  output << result.dump(2) << '\n';
  output.flush();
  fixture::check(output.good());
  return 0;
}
} // namespace
int main(int argc, char **argv) {
  try {
    return run(argc, argv);
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
