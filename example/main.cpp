#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <pqrs/osx/xpc.hpp>
#include <string>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace pqrs::osx::xpc;

int main() {
  try {
    auto time_source = std::make_shared<pqrs::dispatcher::hardware_time_source>();
    auto dispatcher = std::make_shared<pqrs::dispatcher::dispatcher>(time_source);

    // Keep callback state alive until both transports have been destroyed.
    std::promise<bool> listener_ready;
    auto listener_future = listener_ready.get_future();
    std::atomic<bool> listener_finished{false};
    auto finish_listener = [&](bool ready) {
      if (!listener_finished.exchange(true)) {
        listener_ready.set_value(ready);
      }
    };

    std::promise<int> result;
    auto result_future = result.get_future();
    std::atomic<bool> finished{false};
    auto finish = [&](int status) {
      if (!finished.exchange(true)) {
        result.set_value(status);
      }
    };

    // nullopt allows unsigned peers for this local demonstration.
    // Production applications should supply their own signing requirement.
    pqrs::osx::xpc::listener listener(dispatcher,
                                      listener_options({
                                          .common_parameters = {
                                              .signing_requirement = std::nullopt,
                                              .expected_peer_uid = geteuid(),
                                          },
                                          .listener_parameters = {},
                                      }));
    listener.listener_started.connect([&] {
      finish_listener(true);
    });

    listener.listener_failed.connect([&](const auto& error) {
      std::cerr << "Listener failed: " << error.message() << '\n';
      finish_listener(false);
    });
    listener.error_occurred.connect([&](const auto&) {
      std::cerr << "Server transport failed\n";
      finish_listener(false);
      finish(1);
    });
    listener.request_received.connect([&](auto, auto reply, auto data) {
      listener.async_reply(reply,
                           data);
    });
    listener.async_start();
    if (listener_future.wait_for(5s) != std::future_status::ready ||
        !listener_future.get()) {
      std::cerr << "Listener did not start\n";
      return 1;
    }

    pqrs::osx::xpc::client client(dispatcher,
                                  client_options({
                                      .common_parameters = {
                                          .signing_requirement = std::nullopt,
                                          .expected_peer_uid = geteuid(),
                                      },
                                      .client_parameters = {
                                          .endpoint = listener.copy_endpoint(),
                                      },
                                  }));
    client.error_occurred.connect([&](const auto&) {
      std::cerr << "Client transport failed\n";
      finish(1);
    });
    client.connection_failed.connect([&](const auto& error) {
      std::cerr << "Connection failed: " << error.message() << '\n';
      finish(1);
    });
    client.peer_ready.connect([&](auto id, auto) {
      const std::string message = "Hello from cpp-osx-xpc!";
      auto data = std::make_shared<const std::vector<uint8_t>>(message.begin(),
                                                               message.end());
      client.async_request({
          .id = id,
          .data = data,
          .completion = [&, message](auto reply) {
            if (!reply) {
              std::cerr << "Request failed: " << reply.error().message() << '\n';
              finish(1);
            } else if (std::string((*reply)->begin(), (*reply)->end()) != message) {
              std::cerr << "Unexpected echo reply\n";
              finish(1);
            } else {
              std::cout << "Echo reply: " << message << '\n';
              finish(0);
            }
          },
      });
    });
    client.async_start();

    if (result_future.wait_for(10s) != std::future_status::ready) {
      std::cerr << "Timed out waiting for an echo reply\n";
      return 1;
    }

    // Destroy transports on the main thread, after their callbacks return.
    return result_future.get();

  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
