// Offline measurement driver: reuse G2's production graph and external-I/O
// seam.
#include "performance/projection_p1_profile.hpp"
#include <barrier>
#include <binance_market_data/gateway/v1/gateway_service.grpc.pb.h>
#include <fstream>
#include <iostream>
#include <sys/resource.h>
#include <thread>
namespace protocol = binance_market_data::gateway::protocol;
namespace transport = binance_market_data::gateway::transport;
namespace publication = binance_market_data::gateway::publication;
namespace rpc = binance_market_data::gateway::rpc;
namespace production = binance_market_data::gateway::production;
namespace rt = binance_market_data::gateway::runtime;

namespace {
namespace fixture = binance_market_data::gateway::production::four_product_test;
namespace support = production::test_support;
namespace profile_fixture = production::p1_test;
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

std::string frame(const profile_fixture::Workload &work, std::uint64_t id) {
  const auto &key = work.key;
  Json bids = Json::array();
  for (std::size_t j = 0; j < work.batch; ++j) {
    bids.push_back({work.bid(j % work.depth),
                    profile_fixture::multiple(work.step, id % 2U ? 2U : 3U)});
  }
  Json value{{"e", "depthUpdate"},
             {"E", 1700000000000ULL + id},
             {"s", key.symbol},
             {"U", id},
             {"u", id},
             {"b", bids},
             {"a", Json::array()}};
  if (key.market == common::MARKET_SPOT) {
    return Json{
        {"stream",
         transport::make_spot_transport_routes(key.symbol)->diff_depth_stream},
        {"data", value}}
        .dump();
  }
  value["U"] = id - 1U;
  value["pu"] = id - 1U;
  value["T"] = 1700000000001ULL + id;
  return value.dump();
}

support::market::DepthUpdate parse(const rt::MarketKey &key,
                                   const std::string &text) {
  if (key.market == common::MARKET_SPOT) {
    auto value = protocol::parse_combined_event_frame(
        text, transport::sample_real_clock(), key.symbol + "-g1", key.symbol);
    fixture::check(
        std::holds_alternative<protocol::NormalizedMarketEvent>(value));
    return std::get<support::market::DepthUpdate>(
        std::get<protocol::NormalizedMarketEvent>(std::move(value)));
  }
  auto value = protocol::parse_usdm_depth_frame(
      text, transport::sample_real_clock(), key.symbol + "-g1", key.symbol);
  fixture::check(std::holds_alternative<support::market::DepthUpdate>(value));
  return std::get<support::market::DepthUpdate>(std::move(value));
}

struct Client final {
  ::grpc::ClientContext book_context;
  ::grpc::ClientContext event_context;
  std::unique_ptr<::grpc::ClientReader<wire::OrderBookStreamItem>> book;
  std::unique_ptr<::grpc::ClientReader<wire::GatewayEventEnvelope>> events;
  std::vector<std::uint64_t> book_times;
  std::vector<std::uint64_t> event_times;
  std::atomic<std::size_t> received{0U};
  std::atomic<bool> valid{true};
  std::jthread book_thread;
  std::jthread event_thread;
  Client(wire::BinanceMarketDataGatewayService::Stub &stub,
         const rt::MarketKey &key, std::size_t count,
         std::size_t expected_depth, std::chrono::seconds timeout)
      : book_times(count), event_times(count) {
    book_context.set_deadline(std::chrono::system_clock::now() + timeout);
    event_context.set_deadline(std::chrono::system_clock::now() + timeout);
    wire::OrderBookSubscriptionRequest br;
    br.set_request_id("p1-book");
    br.set_schema_version(publication::kOrderBookRequestSchema);
    br.set_venue(key.venue);
    br.set_market(key.market);
    br.set_symbol(key.symbol);
    br.set_initial_snapshot_mode(common::INITIAL_SNAPSHOT_MODE_REQUIRED);
    br.add_supported_snapshot_schema_versions(publication::kSnapshotSchema);
    br.add_supported_update_schema_versions(publication::kUpdateSchema);
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
    er.set_schema_version(publication::kEventRequestSchema);
    auto *selector = er.add_selectors();
    selector->set_venue(key.venue);
    selector->set_market(key.market);
    selector->set_symbol(key.symbol);
    selector->set_stream(common::STREAM_DIFF_DEPTH);
    er.set_delivery_mode(common::DELIVERY_MODE_CONTIGUOUS_EVENTS);
    er.add_supported_payload_schema_versions(publication::kDepthEventSchema);
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
  fixture::check(argc == 6 ||
                 (argc == 5 && std::string{argv[1]} == "--profile"));
  const bool mixed = argc == 5;
  const auto output_path = std::string{argv[mixed ? 4 : 5]};
  std::optional<profile_fixture::Profile> profile;
  auto config = [&] {
    if (mixed) {
      profile = profile_fixture::load(argv[2], argv[3]);
      return profile->configure(BMD_GATEWAY_TWENTY_PRODUCT_CONFIG);
    }
    return fixture::configure_file(BMD_GATEWAY_FOUR_PRODUCT_CONFIG);
  }();
  const auto keys = config.config.market_keys;
  std::vector<profile_fixture::Workload> workloads;
  if (mixed)
    workloads = profile->products;
  else {
    const auto depth = std::stoul(argv[1]);
    const auto batch = std::stoul(argv[2]);
    const auto count = std::stoul(argv[3]);
    const auto interval = std::stoul(argv[4]);
    for (const auto &key : keys)
      workloads.push_back({key, depth, batch, count, interval, "1", "1",
                           10000U - depth, 10000U});
  }
  for (const auto &work : workloads) {
    fixture::check(work.depth > 0U && work.depth <= 10000U && work.batch > 0U &&
                   work.batch <= work.depth && work.count > 0U &&
                   work.count <= 16000U && work.interval <= 1000000U);
  }
  production::GatewayOptions options;
  options.allow_ephemeral_listen_for_testing = true;
  production::ProductionGateway gateway{
      std::move(config.specs), transport::sample_real_clock, "p1-measurement",
      "127.0.0.1:0", options};
  fixture::check(gateway.start().code == production::StartCode::Serving);
  auto stub =
      wire::BinanceMarketDataGatewayService::NewStub(::grpc::CreateChannel(
          "127.0.0.1:" + std::to_string(gateway.observe().selected_port),
          ::grpc::InsecureChannelCredentials()));
  std::vector<std::unique_ptr<Client>> clients;
  std::vector<std::vector<std::string>> frames(keys.size());
  for (std::size_t p = 0; p < keys.size(); ++p) {
    auto *product = gateway.products_for_testing().find(keys[p]);
    const auto &work = workloads[p];
    const auto bootstrap = mixed ? work.sample() : fixture::sample(keys[p]);
    auto seed = fixture::depth(keys[p], 1U, 102U, {}, bootstrap);
    seed.clear_bids();
    seed.clear_asks();
    auto *remove_bid = seed.add_bids();
    remove_bid->set_price(bootstrap.bid);
    remove_bid->set_quantity("0");
    auto *remove_ask = seed.add_asks();
    remove_ask->set_price(bootstrap.ask);
    remove_ask->set_quantity("0");
    for (std::size_t j = 0; j < work.depth; ++j) {
      auto *bid = seed.add_bids();
      bid->set_price(work.bid(j));
      bid->set_quantity(work.step);
      auto *ask = seed.add_asks();
      ask->set_price(work.ask(j));
      ask->set_quantity(work.step);
    }
    fixture::check(product->runtime().submit_depth_update(seed, {1U}) ==
                   rt::AdmissionResult::Accepted);
    fixture::check(product->runtime().observe().last_update_id == 102U);
    clients.push_back(std::make_unique<Client>(
        *stub, keys[p], work.count, work.depth,
        std::chrono::seconds{30 + work.count * work.interval / 1000000U}));
    for (std::size_t i = 0; i < warmup + work.count; ++i) {
      frames[p].push_back(frame(work, first - warmup + i));
    }
  }
  std::vector<std::vector<std::uint64_t>> starts(keys.size());
  for (std::size_t p = 0; p < keys.size(); ++p)
    starts[p].resize(workloads[p].count);
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
            std::make_shared<const protocol::NormalizedMarketEvent>(update), 1U
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
            ,
            trace
#endif
            ) == publication::EventPublishResult::Published);
    fixture::check(product->runtime().submit_depth_update(
                       std::move(update), {1U}
#if defined(BMD_GATEWAY_PERFORMANCE_BASELINE_ENABLED)
                       ,
                       trace
#endif
                       ) == rt::AdmissionResult::Accepted);
  };
  for (std::size_t i = 0; i < warmup; ++i) {
    for (std::size_t p = 0; p < keys.size(); ++p) {
      submit(p, i);
    }
    wait_clients(clients, 2U * (i + 1U));
  }
  std::barrier start_gate{static_cast<std::ptrdiff_t>(keys.size() + 1U)};
  std::atomic<bool> producers_ok{true};
  std::vector<std::jthread> producers;
  for (std::size_t p = 0; p < keys.size(); ++p) {
    producers.emplace_back([&, p] {
      start_gate.arrive_and_wait();
      try {
        const auto begin = Clock::now();
        const auto &work = workloads[p];
        for (std::size_t i = 0; i < work.count; ++i) {
          std::this_thread::sleep_until(
              begin + std::chrono::microseconds{i * work.interval});
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
  for (std::size_t p = 0; p < keys.size(); ++p) {
    const auto deadline = Clock::now() + std::chrono::seconds{10};
    while (clients[p]->received.load() != 2U * (warmup + workloads[p].count)) {
      fixture::check(clients[p]->valid.load() && Clock::now() < deadline);
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  }
  const auto ended = now();
  rusage after{};
  fixture::check(getrusage(RUSAGE_SELF, &after) == 0);
  Json result{{"schema", "projection-p1-offline.v1"},
              {"workload_mode", mixed ? "twenty_mixed" : "four_equal"},
              {"wall_ns", ended - began},
              {"process_cpu_us", cpu_us(after) - cpu_us(before)},
              {"process_cpu_includes",
               "configured owners + producer/parser + loopback "
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
  for (std::size_t p = 0; p < keys.size(); ++p) {
    const auto &work = workloads[p];
    const auto count = work.count;
    const auto observed =
        gateway.products_for_testing().find(keys[p])->runtime().observe();
    fixture::check(observed.last_update_id == first + count - 1U &&
                   observed.state == rt::RuntimeState::Live);
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
         {"depth_per_side", work.depth},
         {"batch_levels", work.batch},
         {"events_per_product", count},
         {"interval_us", work.interval},
         {"book_client_latency", stats(books)},
         {"event_client_latency", stats(events)},
         {"last_update_id", *observed.last_update_id}});
  }
  if (!mixed) {
    const auto &w = workloads.front();
    result["depth_per_side"] = w.depth;
    result["batch_levels"] = w.batch;
    result["events_per_product"] = w.count;
    result["interval_us"] = w.interval;
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
  std::ofstream traces{output_path + ".traces.jsonl"};
  fixture::check(gateway.write_performance_baseline(traces));
  for (std::size_t p = 0; p < keys.size(); ++p) {
    const auto &product = gateway.products_for_testing().products()[p];
    const auto count = workloads[p].count;
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
  std::ofstream output{output_path};
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
