#ifndef CLOUDLAB_RAFT_HH
#define CLOUDLAB_RAFT_HH

#include "cloudlab/kvs.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/network/connection.hh"
#include "cloudlab/network/routing.hh"

#include "cloud.pb.h"

#include <optional>
#include <random>
#include <span>
#include <thread>
#include <unordered_map>
#include <fmt/core.h>
#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>
#include <mutex>

namespace cloudlab {

struct LogEntry {
  uint64_t term_;
  cloud::CloudMessage cmd_;
};

struct PeerIndices {
  uint64_t next_index_;
  uint64_t match_index_;
};

enum class RaftRole {
  LEADER,
  CANDIDATE,
  FOLLOWER,
};

class Raft {
 public:
  explicit Raft(const std::string& path = {}, const std::string& addr = {}, bool open = false)
      : kvs{path, open}, own_addr{addr} {
    // TODO(you)
  }

  auto run(Routing& routing, std::mutex& mtx) -> std::thread;

  auto open() -> bool {
    return kvs.open();
  }

  auto get(const std::string& key, std::string& result) -> bool {
    return kvs.get(key, result);
  }

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer) 
      -> bool {
    return kvs.get_all(buffer);
  }

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool {
    return kvs.remove(key);
  }

  auto leader() -> bool {
    return role == RaftRole::LEADER;
  }

  auto candidate() -> bool {
    return role == RaftRole::CANDIDATE;
  }

  auto follower() -> bool {
    return role == RaftRole::FOLLOWER;
  }

  auto set_leader() -> void {
    role = RaftRole::LEADER;
  }

  auto set_candidate() -> void {
    role = RaftRole::CANDIDATE;
  }
  
  auto set_follower() -> void {
    role = RaftRole::FOLLOWER;
  }

  auto get_role() -> RaftRole {
    return role;
  }
  
  auto term() -> uint64_t {
    return current_term;
  }


  auto election_timeout() -> bool {
    // TODO(you)
  }

  auto reset_election_timer() -> void {
    // TODO(you)
  }


  auto perform_election(Routing& routing) -> void;

  auto heartbeat(Routing& routing, std::mutex& mtx) -> void;

  auto get_dropped_peers(std::vector<std::string>& result) -> void {
    // TODO(you)
    // Return the nodes that have dropped back. 
  }

  auto set_leader_addr(const std::string& addr) -> void {
    leader_addr = addr;
  }

  auto get_leader_addr(std::string& result) -> void {
    result = leader_addr;
  }

 private:
  auto worker(Routing& routing) -> void;
  // the actual kvs
  KVS kvs;

  // every peer is initially a follower
  RaftRole role{RaftRole::FOLLOWER};

  std::string own_addr{""};
  std::string leader_addr{""};


  // persistent state on all servers
  uint64_t current_term{};
  std::optional<SocketAddress> voted_for{};

  // for returning dropped followers
  std::vector<SocketAddress> dropped_peers;

  std::atomic_uint16_t votes_received{0};
  // election timer
  std::chrono::high_resolution_clock::time_point election_timer;
  std::chrono::high_resolution_clock::duration election_timeout_val{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_RAFT_HH