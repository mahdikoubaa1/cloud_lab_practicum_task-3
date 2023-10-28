#include "cloudlab/network/connection.hh"

#include "cloud.pb.h"

#include "argh.hh"
#include <fmt/core.h>

using namespace cloudlab;

auto main(int argc, char *argv[]) -> int {
  cloud::CloudMessage msg{};

  argh::parser cmdl({"-a", "--api"});
  cmdl.parse(argc, argv);

  std::string api_address;
  cmdl({"-a", "--api"}, "127.0.0.1:41000") >> api_address;

  auto num_pos_args = cmdl.pos_args().size();

  msg.set_type(cloud::CloudMessage_Type_REQUEST);

  if (num_pos_args > 3 && cmdl.pos_args().at(1) == "put" &&
      num_pos_args % 2 == 0) {
    msg.set_operation(cloud::CloudMessage_Operation_PUT);
    for (auto i = 2; i < num_pos_args; i += 2) {
      auto *tmp = msg.add_kvp();
      tmp->set_key(cmdl.pos_args().at(i));
      tmp->set_value(cmdl.pos_args().at(i + 1));
    }
  } else if (num_pos_args > 2 && cmdl.pos_args().at(1) == "get") {
    msg.set_operation(cloud::CloudMessage_Operation_GET);
    for (auto i = 2; i < num_pos_args; i++) {
      auto *tmp = msg.add_kvp();
      tmp->set_key(cmdl.pos_args().at(i));
    }
  } else if (num_pos_args > 2 && cmdl.pos_args().at(1) == "direct_get") {
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_DIRECT_GET);
    for (auto i = 2; i < num_pos_args; i++) {
      auto *tmp = msg.add_kvp();
      tmp->set_key(cmdl.pos_args().at(i));
    }
  } else if (num_pos_args > 2 && cmdl.pos_args().at(1) == "del") {
    msg.set_operation(cloud::CloudMessage_Operation_DELETE);
    for (auto i = 2; i < num_pos_args; i++) {
      auto *tmp = msg.add_kvp();
      tmp->set_key(cmdl.pos_args().at(i));
    }
  } else if (num_pos_args == 3 && cmdl.pos_args().at(1) == "join") {
    msg.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
    auto *address = msg.mutable_address();
    address->set_address(cmdl.pos_args().at(2));
  } else if (num_pos_args == 2 && cmdl.pos_args().at(1) == "dropped") {
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_DROPPED_NODE);
  } else if (num_pos_args == 2 && cmdl.pos_args().at(1) == "leader") {
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_GET_LEADER);
  } else {
    fmt::print("Usage: {} <operation> <args>\n", cmdl.pos_args().at(0));
    return 1;
  }

  Connection con{api_address};

  // send request
  con.send(msg);

  // receive reply
  con.receive(msg);

  switch (msg.operation()) {
    case cloud::CloudMessage_Operation_PUT:
    case cloud::CloudMessage_Operation_GET:
    case cloud::CloudMessage_Operation_DELETE:
    case cloud::CloudMessage_Operation_RAFT_DIRECT_GET:
      if (!msg.success()) {
        fmt::print("{}\n", msg.message());
      } else {
        for (const auto &kvp : msg.kvp()) {
          fmt::print("Key:\t{}\nValue:\t{}\n", kvp.key(), kvp.value());
        }
      }
      break;
    case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE:
      if (!msg.success()) {
        fmt::print("{}\n", msg.message());
      } else {
        for (const auto &kvp : msg.kvp()) {
          fmt::print("Dropped:\t{}\n", kvp.value());
        }
      }
      break;
    default:
      fmt::print("{}\n", msg.message());
      break;
  }
}
