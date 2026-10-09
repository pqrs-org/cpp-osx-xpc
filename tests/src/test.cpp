#include <Security/Security.h>
#include <atomic>
#include <boost/ut.hpp>
#include <cerrno>
#include <cstdint>
#include <future>
#include <iostream>
#include <pqrs/cf/cf_ptr.hpp>
#include <pqrs/cf/string.hpp>
#include <pqrs/osx/xpc.hpp>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

using namespace std::chrono_literals;
using namespace pqrs::osx::xpc;

namespace {
template <typename T>
concept has_listener_api = requires(T& value) {
  value.copy_endpoint();
  value.listener_started;
  value.listener_failed;
};

template <typename T>
concept has_client_api = requires(T& value) {
  value.connection_failed;
};

// Keep role-specific APIs and options unavailable on the other transport role.
static_assert(has_listener_api<listener> &&
              !has_listener_api<client>);
static_assert(has_client_api<client> &&
              !has_client_api<listener>);
static_assert(!std::is_constructible_v<listener,
                                       std::weak_ptr<pqrs::dispatcher::dispatcher>,
                                       client_options>);
static_assert(!std::is_constructible_v<client,
                                       std::weak_ptr<pqrs::dispatcher::dispatcher>,
                                       listener_options>);

bool wait_for(std::string_view description,
              const std::function<bool()>& predicate) {
  std::cerr << "Waiting for " << description << "...\n";

  auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      std::cerr << "Timed out waiting for " << description << ".\n";
      return false;
    }

    std::this_thread::sleep_for(10ms);
  }

  return true;
}

std::string self_requirement() {
  // Use this executable's designated requirement so both test peers can authenticate each other.
  SecCodeRef raw_code = nullptr;
  if (SecCodeCopySelf(kSecCSDefaultFlags,
                      &raw_code) != errSecSuccess) {
    throw std::runtime_error("cannot identify test process");
  }

  auto code = pqrs::cf::adopt_cf_ptr(raw_code);
  SecRequirementRef raw_requirement = nullptr;
  if (SecCodeCopyDesignatedRequirement(*code,
                                       kSecCSDefaultFlags,
                                       &raw_requirement) != errSecSuccess) {
    throw std::runtime_error("cannot obtain test signature requirement");
  }

  auto requirement = pqrs::cf::adopt_cf_ptr(raw_requirement);
  CFStringRef raw_string = nullptr;
  if (SecRequirementCopyString(*requirement,
                               kSecCSDefaultFlags,
                               &raw_string) != errSecSuccess) {
    throw std::runtime_error("cannot serialize test signature requirement");
  }

  auto string = pqrs::cf::adopt_cf_ptr(raw_string);
  return pqrs::cf::make_string(*string).value();
}

object start_listener(listener& server) {
  // Wait for listener startup before handing its anonymous endpoint to a client.
  auto ready = std::make_shared<std::promise<void>>();
  auto future = ready->get_future();
  auto connection = server.listener_started.connect([ready] {
    ready->set_value();
  });
  server.async_start();

  std::cerr << "Waiting for the listener to start...\n";
  if (future.wait_for(5s) != std::future_status::ready) {
    throw std::runtime_error("XPC listener failed to start");
  }

  connection.disconnect();

  return server.copy_endpoint();
}
} // namespace

int main() {
  using namespace boost::ut;
  auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
  auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

  "options validate common limits and client retry intervals"_test = [&] {
    // Accept the defaults, an unlimited payload size, and an empty-only payload limit.
    expect(listener_options{}.validate());
    expect(client_options{}.validate());

    expect(listener_options({
                                .max_message_size = std::nullopt,
                            })
               .validate());

    expect(client_options({
                              .max_message_size = std::nullopt,
                          })
               .validate());

    expect(listener_options({
                                .max_message_size = 0,
                            })
               .validate());

    expect(client_options({
                              .max_message_size = 0,
                          })
               .validate());

    // Reject nonpositive request timeouts both during validation and construction.
    auto listener = listener_options({
        .request_timeout = 0ms,
    });
    expect(!listener.validate());

    bool listener_rejected = false;
    try {
      pqrs::osx::xpc::listener invalid(dispatcher,
                                       listener);
    } catch (const std::invalid_argument&) {
      listener_rejected = true;
    }
    expect(listener_rejected);

    expect(!listener_options({
                                 .request_timeout = 0ms,
                             })
                .validate());

    expect(!client_options({
                               .request_timeout = -1ms,
                           })
                .validate());

    // Reject nonpositive tick intervals so the periodic timer cannot busy-loop.
    expect(!listener_options({
                                 .tick_interval = 0ms,
                             })
                .validate());

    expect(!client_options({
                               .tick_interval = -1ms,
                           })
                .validate());

    // Reject nonpositive reconnect intervals both during validation and construction.
    auto client = client_options({
        .common_parameters = {},
        .client_parameters = {
            .reconnect_interval = 0ms,
        },
    });
    expect(!client.validate());

    bool client_rejected = false;
    try {
      pqrs::osx::xpc::client invalid(dispatcher,
                                     client);
    } catch (const std::invalid_argument&) {
      client_rejected = true;
    }
    expect(client_rejected);

    expect(!client_options({
                               .common_parameters = {},
                               .client_parameters = {
                                   .reconnect_interval = -1ms,
                               },
                           })
                .validate());
  };

  "empty connection operations are safe and report missing connection"_test = [] {
    // Exercise configuration and inspection without a native connection.
    connection empty;
    empty.set_target_queue(nullptr);
    empty.set_event_handler(^(xpc_object_t){});
    expect(empty.set_peer_code_signing_requirement("anchor apple") == EINVAL);
    expect(empty.get() == nullptr);
    expect(!empty.copy_endpoint());
    expect(!empty.get_peer_uid());

    // Sending without a connection must report failure without calling XPC.
    dictionary message;
    expect(!empty.send_message(message));
    expect(!empty.send_message_with_reply({
        .message = message,
        .queue = nullptr,
        .handler = ^(xpc_object_t){},
    }));
  };

  "object and dictionary bool conversion reflects ownership"_test = [] {
    // Empty wrappers must be false; borrowing and moving must preserve valid ownership.
    object empty;
    expect(!empty);

    object null(nullptr);
    expect(!null);

    auto owned = pqrs::osx::adopt_xpc_object(xpc_dictionary_create(nullptr,
                                                                   nullptr,
                                                                   0));
    expect(static_cast<bool>(owned));

    object borrowed(owned.get());
    auto moved = std::move(owned);
    expect(!owned);
    expect(static_cast<bool>(moved));
    expect(static_cast<bool>(borrowed));

    // Moving a dictionary must leave only the destination usable.
    dictionary source;
    expect(static_cast<bool>(source));
    auto destination = std::move(source);
    expect(!source);
    expect(static_cast<bool>(destination));
  };

  "dictionary retains events and distinguishes missing or mistyped values"_test = [] {
    // Wrap a borrowed dictionary and let the original wrapper leave scope.
    auto retained = [] {
      dictionary source;
      source.set_empty_data("empty");
      source.set_data("payload",
                      std::vector<uint8_t>{1, 2, 3});
      source.set_bool("flag",
                      false);
      return dictionary::from_xpc_object(source.get());
    }();
    expect(retained.has_value()) << fatal;

    // Read valid values after the original wrapper has been destroyed.
    auto empty = retained->get_data("empty",
                                    {
                                        .max_size = 3,
                                    });
    expect(empty.has_value() &&
           empty->empty());

    auto payload = retained->get_data("payload",
                                      {
                                          .max_size = 3,
                                      });
    expect(payload.has_value()) << fatal;
    expect(*payload == std::vector<uint8_t>{1, 2, 3});
    expect(retained->get_bool("flag") == std::optional<bool>(false));

    // Distinguish absent or incorrectly typed entries from valid values.
    expect(!retained->get_bool("missing"));
    expect(!retained->get_bool("payload"));

    auto missing = retained->get_data("missing",
                                      {
                                          .max_size = 3,
                                      });
    expect(!missing &&
           missing.error() == errc::invalid_message);

    auto mistyped = retained->get_data("flag",
                                       {
                                           .max_size = 3,
                                       });
    expect(!mistyped &&
           mistyped.error() == errc::invalid_message);

    // Check unlimited reads and the inclusive payload-size boundary, including zero.
    auto unlimited = retained->get_data("payload",
                                        {});
    expect(unlimited &&
           *unlimited == std::vector<uint8_t>{1, 2, 3});

    auto oversized = retained->get_data("payload",
                                        {
                                            .max_size = 2,
                                        });
    expect(!oversized &&
           oversized.error() == errc::message_too_large);

    auto zero_limit = retained->get_data("payload",
                                         {
                                             .max_size = 0,
                                         });
    expect(!zero_limit &&
           zero_limit.error() == errc::message_too_large);

    auto empty_with_zero_limit = retained->get_data("empty",
                                                    {
                                                        .max_size = 0,
                                                    });
    expect(empty_with_zero_limit &&
           empty_with_zero_limit->empty());

    // Returned payloads must survive mutation and destruction of the dictionary.
    retained->set_data("payload",
                       std::vector<uint8_t>{9});
    expect(*payload == std::vector<uint8_t>{1, 2, 3});

    retained.reset();
    expect(*payload == std::vector<uint8_t>{1, 2, 3});
    expect(empty.has_value() &&
           empty->empty());

    // Reject null objects and XPC objects that are not dictionaries.
    expect(!dictionary::from_xpc_object(nullptr));
    expect(!dictionary::from_xpc_object(
        const_cast<void*>(static_cast<const void*>(XPC_ERROR_CONNECTION_INVALID))));
  };

  "dictionary message type access rejects missing and mistyped values"_test = [] {
    // Require a uint64 entry while accepting the full uint64 range, including zero.
    dictionary message;
    expect(!message.get_uint64("type"));

    message.set_bool("type", true);
    expect(!message.get_uint64("type"));

    message.set_uint64("type", 0);
    expect(message.get_uint64("type") == std::optional<uint64_t>(0));

    message.set_uint64("type", UINT64_MAX);
    expect(message.get_uint64("type") == std::optional<uint64_t>(UINT64_MAX));
  };

  "peer interruption invalidates the session and pending requests block retries"_test = [] {
    // Mark the initial session ready and leave one request pending.
    peer value{connection{}};
    auto now = std::chrono::steady_clock::now();
    auto session = value.get_session();
    expect(value.mark_ready_for_communication());
    expect(!value.mark_ready_for_communication());
    value.add_pending_request(1,
                              {
                                  .completion = {},
                                  .deadline = now + 1s,
                                  .handshake = false,
                              });

    // The first interruption replaces the session; repeated interruptions must not replace it again.
    expect(value.mark_connection_interrupted(now + 10ms));
    expect(!value.ready_for_communication());
    expect(!value.matches_session(session));
    auto interrupted_session = value.get_session();
    expect(!value.mark_connection_interrupted(now + 2s));
    expect(value.matches_session(interrupted_session));

    // Pending requests block handshake retries until removed, even after their deadline.
    expect(!value.can_retry_handshake(now + 10ms));
    expect(!value.has_expired_pending_request(now));
    expect(value.has_expired_pending_request(now + 1s));
    expect(value.take_pending_request(1).has_value());
    expect(!value.take_pending_request(1).has_value());

    // Once requests are removed, retry only after the interruption delay and while not ready.
    expect(!value.can_retry_handshake(now));
    expect(value.can_retry_handshake(now + 10ms));
    expect(value.mark_ready_for_communication());
    expect(!value.can_retry_handshake(now + 1s));
    expect(value.mark_connection_interrupted(now));
    expect(!value.matches_session(interrupted_session));
  };

  "request reply notifications peer uid and size limit"_test = [&] {
    // Connect peers that trust this executable and enforce a four-byte payload limit.
    std::atomic<peer_id> server_peer{0}, client_peer{0};
    std::atomic<bool> uid_matches{false}, on_dispatcher{false}, notified{false};
    std::atomic<int> request_count{0}, completions{0};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {
                                            .signing_requirement = self_requirement(),
                                            .max_message_size = 4,
                                        },
                                        .listener_parameters = {},
                                    }));
    server.peer_ready.connect([&](auto id,
                                  auto uid) {
      server_peer = id;
      uid_matches = uid == geteuid();
      on_dispatcher = server.dispatcher_thread();
    });
    server.request_received.connect([&](auto id,
                                        auto request,
                                        auto data) {
      ++request_count;
      server.async_reply(request,
                         data);
      server.async_send(id,
                        std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{4, 3, 2, 1}));
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .signing_requirement = self_requirement(),
                                          .expected_peer_uid = geteuid(),
                                          .max_message_size = 4,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  const auto&) {
      client_peer = id;
    });
    client.message_received.connect([&](auto,
                                        auto data) {
      notified = *data == std::vector<uint8_t>{4, 3, 2, 1};
    });

    // Wait for the handshake and verify the peer UID and notification thread.
    client.async_start();

    expect(wait_for("both peers to become ready", [&] {
      return client_peer != 0 && server_peer != 0;
    })) << fatal;
    expect(uid_matches.load());
    expect(on_dispatcher.load());

    // Send one request at the size limit and one above it; also receive a separate notification.
    std::atomic<bool> roundtrip{false}, oversized{false};
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3, 4}),
        .completion = [&](auto result) {
          roundtrip = result &&
                      **result == std::vector<uint8_t>{1, 2, 3, 4} &&
                      client.dispatcher_thread();
          ++completions;
        },
    });
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3, 4, 5}),
        .completion = [&](auto result) {
          oversized = !result &&
                      result.error() == make_error_code(errc::message_too_large);
          ++completions;
        },
    });

    // Verify dispatcher-thread completion, local size rejection, and only one request reaching the server.
    expect(wait_for("both request completions and the notification", [&] {
      return completions == 2 && notified;
    }));
    expect(roundtrip.load());
    expect(oversized.load());
    expect(request_count == 1);
  };

  "zero payload limit permits empty requests and rejects nonempty requests"_test = [&] {
    // Connect an echo server and client that allow only empty payloads.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> completions{0}, requests{0};
    std::atomic<bool> empty_reply{false}, rejected{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .max_message_size = 0,
                                    }));
    server.request_received.connect([&](auto,
                                        auto token,
                                        auto data) {
      ++requests;
      server.async_reply(token,
                         data);
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .max_message_size = 0,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  auto) {
      client_peer = id;
    });

    client.async_start();

    expect(wait_for("the empty-payload client to become ready", [&] { return client_peer != 0; })) << fatal;

    // An empty request must round trip; a nonempty request must be rejected locally.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          empty_reply = result &&
                        (*result)->empty();
          ++completions;
        },
    });
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1}),
        .completion = [&](auto result) {
          rejected = !result &&
                     result.error() == make_error_code(errc::message_too_large);
          ++completions;
        },
    });

    // Both completions must run, but only the empty request may reach the server.
    expect(wait_for("both payload-limit request completions", [&] {
      return completions == 2;
    })) << fatal;
    expect(empty_reply.load());
    expect(rejected.load());
    expect(requests == 1);
  };

  "unlimited payloads round trip beyond the default limit"_test = [&] {
    // Disable the payload limit on both sides and establish an echo connection.
    std::atomic<peer_id> client_peer{0};
    std::atomic<bool> completed{false}, matches{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .max_message_size = std::nullopt,
                                    }));
    server.request_received.connect([&](auto,
                                        auto token,
                                        auto data) {
      server.async_reply(token,
                         data);
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .max_message_size = std::nullopt,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  auto) {
      client_peer = id;
    });

    client.async_start();

    expect(wait_for("the unlimited-payload client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // Round trip 64 KiB, exceeding the default 32 KiB limit, without changing the data.
    auto payload = std::make_shared<const std::vector<uint8_t>>(64 * 1024, 42);
    client.async_request({
        .id = client_peer,
        .data = payload,
        .completion = [&, payload](auto result) {
          matches = result &&
                    **result == *payload;
          completed = true;
        },
    });

    expect(wait_for("the large-payload reply", [&] {
      return completed.load();
    })) << fatal;
    expect(matches.load());
  };

  "timeout disconnects and reconnects with a new peer"_test = [&] {
    // Keep the first reply token unanswered and reply normally to later requests.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> connections{0}, closed{0}, completed{0};
    std::atomic<bool> timed_out{false}, retried{false};
    std::optional<reply_token> unanswered;

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto reply,
                                        auto) {
      if (!unanswered) {
        unanswered = reply;
      } else {
        server.async_reply(reply,
                           std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{42}));
      }
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .request_timeout = 100ms,
                                          .tick_interval = 20ms,
                                      },
                                      .client_parameters = {
                                          .reconnect_interval = 10ms,
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  const auto&) {
      client_peer = id;
      ++connections;
    });
    client.peer_invalidated.connect([&](auto,
                                        const auto&) {
      ++closed;
    });

    client.async_start();

    expect(wait_for("the initial connection", [&] {
      return connections == 1;
    })) << fatal;

    // Let the first request time out and verify invalidation and reconnection with a new peer ID.
    auto original_peer = client_peer.load();

    client.async_request({
        .id = original_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          timed_out = !result &&
                      result.error() == make_error_code(errc::request_timeout);
          ++completed;
        },
    });

    expect(wait_for("the timeout completion and reconnection", [&] {
      return connections >= 2 && completed == 1;
    }));
    expect(timed_out.load());
    expect(closed >= 1);
    expect(client_peer != original_peer);

    // Attempt a stale reply from the old session, then verify a new request gets only its own reply.
    server.enqueue_to_dispatcher([&] {
      server.async_reply(*unanswered,
                         std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{99}));
    });

    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          retried = result &&
                    **result == std::vector<uint8_t>{42};
          ++completed;
        },
    });

    expect(wait_for("the reply after reconnection", [&] {
      return completed == 2;
    }));
    expect(retried.load());
  };

  "oversized incoming payload is rejected before application dispatch"_test = [&] {
    // Give the client a larger limit so it can send a payload the server must reject.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> requests{0}, closed{0};
    std::atomic<bool> aborted{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {
                                            .max_message_size = 4,
                                        },
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto,
                                        auto) {
      ++requests;
    });
    server.peer_invalidated.connect([&](auto,
                                        const auto&) {
      ++closed;
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .max_message_size = 8,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  const auto&) {
      client_peer = id;
    });
    client.async_start();

    expect(wait_for("the size-limit client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // Send five bytes to the four-byte server and verify rejection before application dispatch.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3, 4, 5}),
        .completion = [&](auto result) {
          aborted = !result &&
                    (result.error() == make_error_code(errc::connection_invalid) ||
                     result.error() == make_error_code(errc::connection_interrupted));
        },
    });

    expect(wait_for("peer invalidation and the oversized-request failure", [&] {
      return closed != 0 &&
             aborted;
    }));
    expect(requests == 0);
  };

  "native replies correlate out of order and are single use"_test = [&] {
    // Collect two reply tokens, reply in reverse order, and attempt to reuse the first token.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> completed{0};
    std::atomic<bool> first_matches{false}, second_matches{false};
    std::vector<reply_token> replies;

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto reply,
                                        auto) {
      replies.push_back(reply);
      if (replies.size() == 2) {
        server.async_reply(replies[1],
                           std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{2}));
        server.async_reply(replies[0],
                           std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{1}));
        server.async_reply(replies[0],
                           std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{99}));
      }
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {},
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  const auto&) {
      client_peer = id;
    });

    client.async_start();

    expect(wait_for("the reply-order client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // Submit two requests and verify each completion receives its corresponding reply exactly once.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          first_matches = result &&
                          **result == std::vector<uint8_t>{1};
          ++completed;
        },
    });
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          second_matches = result &&
                           **result == std::vector<uint8_t>{2};
          ++completed;
        },
    });

    expect(wait_for("both out-of-order replies", [&] {
      return completed == 2;
    }));
    expect(first_matches.load());
    expect(second_matches.load());
  };

  "reply tokens cannot be consumed by another transport with the same peer id"_test = [&] {
    // Use two transports with matching numeric peer IDs to test token ownership across transports.
    std::atomic<peer_id> server_peer{0}, client_peer{0};
    std::atomic<bool> completed{false}, matches{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    server.peer_ready.connect([&](auto id,
                                  auto) {
      server_peer = id;
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {},
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  auto) {
      client_peer = id;
    });

    // Try replying from the nonowning transport before the owning listener sends the valid reply.
    server.request_received.connect([&](auto,
                                        auto reply,
                                        auto) {
      client.async_reply(reply,
                         std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{99}));

      // An invalid-ID request completes after the wrong client's queued reply.
      // Only then attempt the valid reply on the owning listener.
      client.async_request({
          .id = 0,
          .data = std::make_shared<const std::vector<uint8_t>>(),
          .completion = [&, reply](auto result) {
            expect(!result &&
                   result.error() == make_error_code(errc::not_ready));

            server.async_reply(reply,
                               std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{42}));
          },
      });
    });

    // Confirm the IDs match, then verify the wrong transport did not consume the reply token.
    client.async_start();

    expect(wait_for("both token-ownership peers to become ready", [&] {
      return server_peer != 0 &&
             client_peer != 0;
    })) << fatal;

    expect(server_peer.load() == client_peer.load()) << fatal;

    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          matches = result &&
                    **result == std::vector<uint8_t>{42};
          completed = true;
        },
    });

    expect(wait_for("the reply from the owning listener", [&] {
      return completed.load();
    }));

    expect(matches.load());
  };

  "discarding a reply does not interrupt the connection"_test = [&] {
    // Discard the first reply token and reply to the next request on the same connection.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> requests{0}, interruptions{0}, completed{0};
    std::atomic<bool> discarded{false}, replied{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto reply,
                                        auto) {
      if (++requests > 1) {
        server.async_reply(reply,
                           std::make_shared<const std::vector<uint8_t>>(std::vector<uint8_t>{7}));
      }
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {},
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  const auto&) {
      client_peer = id;
    });
    client.peer_interrupted.connect([&](auto) {
      ++interruptions;
    });

    client.async_start();

    expect(wait_for("the discarded-reply client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // The discarded token must fail only its request with connection_interrupted.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          discarded = !result &&
                      result.error() == make_error_code(errc::connection_interrupted);
          ++completed;
        },
    });

    expect(wait_for("the discarded-reply failure", [&] {
      return completed == 1;
    }));

    // After the request failure, verify the same peer remains usable without an interruption signal.
    expect(discarded.load());

    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto result) {
          replied = result &&
                    **result == std::vector<uint8_t>{7};
          ++completed;
        },
    });

    expect(wait_for("the next reply on the same connection", [&] {
      return completed == 2;
    }));

    expect(replied.load());
    expect(interruptions == 0);
  };

  "client verifies the signature of native handshake replies"_test = [&] {
    // Require a signature that the listener cannot satisfy when returning the handshake reply.
    std::atomic<int> admitted{0}, rejected{0};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));

    pqrs::osx::xpc::client client(
        dispatcher,
        client_options({
            .common_parameters = {
                .signing_requirement = "identifier \"org.pqrs.untrusted-never-match\"",
            },
            .client_parameters = {
                .endpoint = start_listener(server),
            },
        }));
    client.peer_ready.connect([&](auto,
                                  const auto&) {
      ++admitted;
    });
    client.peer_invalidated.connect([&](auto,
                                        const auto& error) {
      if (error == make_error_code(errc::signature_rejected)) {
        ++rejected;
      }
    });

    // Verify signature rejection is reported and the client never admits the peer.
    client.async_start();

    expect(wait_for("handshake signature rejection", [&] {
      return rejected != 0;
    }));

    expect(admitted == 0);
  };

  "UID mismatch does not admit client"_test = [&] {
    // Require a UID different from the actual listener process UID.
    std::atomic<int> admitted{0}, failed{0};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .expected_peer_uid = static_cast<uid_t>(geteuid() + 1),
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto,
                                  const auto&) {
      ++admitted;
    });
    client.peer_invalidated.connect([&](auto,
                                        const auto&) {
      ++failed;
    });
    client.connection_failed.connect([&](const auto&) {
      ++failed;
    });

    // Verify connection failure or invalidation occurs before the peer becomes ready.
    client.async_start();

    expect(wait_for("UID mismatch rejection", [&] {
      return failed != 0;
    }));

    expect(admitted == 0);
  };

  "signature requirement rejects a peer before application messages"_test = [&] {
    // Configure the listener to reject the connecting executable before accepting application traffic.
    std::atomic<int> admitted{0}, requests{0}, failed{0};

    pqrs::osx::xpc::listener server(
        dispatcher,
        listener_options({
            .common_parameters = {
                .signing_requirement = "identifier \"org.pqrs.untrusted-never-match\"",
            },
            .listener_parameters = {},
        }));
    server.peer_ready.connect([&](auto,
                                  const auto&) {
      ++admitted;
    });
    server.request_received.connect([&](auto,
                                        auto,
                                        auto) {
      ++requests;
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .request_timeout = 100ms,
                                          .tick_interval = 20ms,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_invalidated.connect([&](auto,
                                        const auto&) {
      ++failed;
    });
    client.connection_failed.connect([&](const auto&) {
      ++failed;
    });

    // Verify the client observes failure and the listener admits no peer or request.
    client.async_start();

    expect(wait_for("listener signature rejection", [&] {
      return failed != 0;
    }));
    expect(admitted == 0);
    expect(requests == 0);
  };

  "invalid signature requirement fails closed"_test = [&] {
    // Provide a malformed signing requirement and observe listener startup signals.
    std::atomic<int> bound{0}, failed{0};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {
                                            .signing_requirement = "not a valid requirement ???",
                                        },
                                        .listener_parameters = {},
                                    }));
    server.listener_started.connect([&] {
      ++bound;
    });
    server.listener_failed.connect([&](const auto&) {
      ++failed;
    });

    // Startup must fail rather than silently accepting connections without the requirement.
    server.async_start();

    expect(wait_for("invalid signing requirement startup failure", [&] {
      return failed != 0;
    }));

    expect(bound == 0);
  };

  "signal exceptions report a terminal failure and allow deferred recreation"_test = [&] {
    // Throw from listener_started and recreate the failed listener in a later dispatcher task.
    std::atomic<int> failures{0};
    std::atomic<bool> correct_error{false}, on_dispatcher{false}, restarted{false};

    pqrs::osx::xpc::listener parent(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));

    auto server = std::make_unique<listener>(dispatcher,
                                             listener_options({
                                                 .common_parameters = {},
                                                 .listener_parameters = {},
                                             }));
    server->listener_started.connect([] {
      throw std::runtime_error("listener callback failed");
    });
    server->error_occurred.connect([&](const error_info& error) {
      ++failures;
      on_dispatcher = server->dispatcher_thread();
      correct_error = error.code == make_error_code(errc::unexpected_exception) &&
                      error.message == "Unexpected XPC transport exception";
      parent.enqueue_to_dispatcher([&] {
        server = std::make_unique<listener>(dispatcher,
                                            listener_options({
                                                .common_parameters = {},
                                                .listener_parameters = {},
                                            }));
        server->listener_started.connect([&] {
          restarted = true;
        });
        server->async_start();
      });
    });

    // Verify one terminal error on the dispatcher thread and successful deferred recreation.
    server->async_start();

    expect(wait_for("the recreated listener to start", [&] {
      return restarted.load();
    })) << fatal;

    expect(failures == 1);
    expect(correct_error.load());
    expect(on_dispatcher.load());
  };

  "completion exceptions are reported without escaping the dispatcher"_test = [&] {
    // Establish an echo connection and observe terminal errors from the client.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> failures{0}, completions{0};
    std::atomic<bool> correct_error{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {},
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto token,
                                        auto data) {
      server.async_reply(token,
                         data);
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {},
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  auto) {
      client_peer = id;
    });
    client.error_occurred.connect([&](const error_info& error) {
      correct_error = error.code == make_error_code(errc::unexpected_exception) &&
                      error.message == "Unexpected XPC transport exception";
      ++failures;
    });

    client.async_start();

    expect(wait_for("the throwing-completion client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // Throw from the reply completion and verify a single error notification without escaping the dispatcher.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [&](auto) {
          ++completions;
          throw std::runtime_error("completion failed");
        },
    });

    expect(wait_for("the reply completion exception report", [&] {
      return failures != 0;
    })) << fatal;

    expect(correct_error.load());
    expect(completions == 1);
    expect(failures == 1);
  };

  "callback failure before start is reported"_test = [&] {
    // Observe errors on a client that has not been started.
    std::atomic<int> failures{0};

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {},
                                      .client_parameters = {},
                                  }));
    client.error_occurred.connect([&](const error_info& error) {
      expect(error.code == make_error_code(errc::unexpected_exception));
      ++failures;
    });

    // Throw from a not-ready completion and verify that this path also reports the exception once.
    client.async_request({
        .id = 1,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = [](auto) {
          throw std::runtime_error("not-ready callback failed");
        },
    });

    expect(wait_for("the not-ready completion exception report", [&] {
      return failures != 0;
    }));

    expect(failures == 1);
  };

  "exceptions on the XPC queue stop work without replaying application requests"_test = [&] {
    // Allow submission on this thread but throw when the completion is copied on another thread.
    struct throwing_copy {
      std::thread::id submitting_thread{std::this_thread::get_id()};

      throwing_copy() = default;

      throwing_copy(const throwing_copy& other)
          : submitting_thread(other.submitting_thread) {
        if (std::this_thread::get_id() != submitting_thread) {
          throw std::bad_alloc();
        }
      }

      void operator()(std::expected<pqrs::not_null_shared_ptr_t<const std::vector<uint8_t>>,
                                    std::error_code>) const {}
    };

    // Connect peers and record application requests and dispatcher-thread error notifications.
    std::atomic<peer_id> client_peer{0};
    std::atomic<int> failures{0}, requests{0};
    std::atomic<bool> generic_error{false}, on_dispatcher{false};

    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {
                                            .tick_interval = 20ms,
                                        },
                                        .listener_parameters = {},
                                    }));
    server.request_received.connect([&](auto,
                                        auto,
                                        auto) {
      ++requests;
    });

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .tick_interval = 20ms,
                                      },
                                      .client_parameters = {
                                          .endpoint = start_listener(server),
                                      },
                                  }));
    client.peer_ready.connect([&](auto id,
                                  auto) {
      client_peer = id;
    });
    client.error_occurred.connect([&](const error_info& error) {
      on_dispatcher = client.dispatcher_thread();
      generic_error = error.code == make_error_code(errc::unexpected_exception);
      ++failures;
    });

    client.async_start();

    expect(wait_for("the throwing-copy client to become ready", [&] {
      return client_peer != 0;
    })) << fatal;

    // Trigger the copy failure on the XPC queue, then attempt further sends and a restart.
    client.async_request({
        .id = client_peer,
        .data = std::make_shared<const std::vector<uint8_t>>(),
        .completion = throwing_copy{},
    });

    expect(wait_for("the XPC queue exception report", [&] {
      return failures != 0;
    })) << fatal;

    client.async_send(client_peer,
                      std::make_shared<const std::vector<uint8_t>>());

    client.async_start();

    std::cerr << "Waiting 200ms to verify work remains stopped across timer ticks...\n";
    std::this_thread::sleep_for(200ms); // observe multiple 20ms ticks while work stays stopped

    // The terminal failure must be reported once and all application work must remain stopped.
    expect(failures == 1);
    expect(requests == 0);
    expect(generic_error.load());
    expect(on_dispatcher.load());
  };

  // Exercise queued events/timers during destruction under AddressSanitizer.
  "shutdown while connection is starting"_test = [&] {
    // Keep a listener alive while repeatedly destroying clients immediately after starting them.
    pqrs::osx::xpc::listener server(dispatcher,
                                    listener_options({
                                        .common_parameters = {
                                            .tick_interval = 20ms,
                                        },
                                        .listener_parameters = {},
                                    }));
    auto endpoint = start_listener(server);
    for (int i = 0; i < 20; ++i) {
      pqrs::osx::xpc::client client(dispatcher,
                                    client_options({
                                        .common_parameters = {
                                            .tick_interval = 20ms,
                                        },
                                        .client_parameters = {
                                            .endpoint = endpoint,
                                        },
                                    }));
      client.async_start();
    }

    // Allow several 20ms timer intervals for queued callbacks to expose lifetime errors under AddressSanitizer.
    std::cerr << "Waiting 200ms for callbacks after client destruction...\n";
    std::this_thread::sleep_for(200ms);
    expect(true);
  };
}
