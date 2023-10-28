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
#include <fstream>

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
        explicit Raft(const std::string &path = {}, const std::string &addr = {}, bool open = false)
                : kvs{path, open}, own_addr{addr} {
            current_term=0;
        }


        auto run(Routing &routing, std::mutex &mtx) -> std::thread;

        auto open() -> bool {
            return kvs.open();
        }

        auto get(const std::string &key, std::string &result) -> bool {
            return kvs.get(key, result);
        }

        auto get_all(std::vector<std::pair<std::string, std::string>> &buffer)
        -> bool {
            return kvs.get_all(buffer);
        }

        auto put(const std::string &key, const std::string &value) -> bool;

        auto remove(const std::string &key) -> bool {
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
            leader_addr = own_addr;
        }

        auto set_candidate() -> void {
            role = RaftRole::CANDIDATE;
            leader_addr = "";
        }

        auto set_follower() -> void {
            role = RaftRole::FOLLOWER;
            leader_addr = "";
        }

        auto get_role() -> RaftRole {
            return role;
        }

        auto term() -> uint64_t {
            return current_term;
        }


        auto election_timeout() -> bool {
            return election_timer < std::chrono::high_resolution_clock::now();
        }

        auto reset_election_timer() -> void {
            std::random_device dev;
            std::mt19937 rng(dev());
            std::uniform_int_distribution<std::mt19937::result_type> dist(2000, 4000);
            election_timeout_val = std::chrono::high_resolution_clock::duration(std::chrono::milliseconds(dist(rng)));
            election_timer = std::chrono::high_resolution_clock::now() + election_timeout_val;
        }

        auto set_term(uint64_t newterm) -> void {
            current_term = newterm;
        }

        auto set_voted_for(const SocketAddress &addr) -> void {
            voted_for = addr;
        }

        auto perform_election(Routing &routing, std::mutex &mtx) -> void;

        auto heartbeat(Routing &routing, std::mutex &mtx) -> void;

        auto get_dropped_peers(std::vector<std::string> &result) -> void {
            for (auto &peer: dropped_peers) {
                result.emplace_back(peer.string());
            }
            // Return the nodes that have dropped back.
        }

        auto set_leader_addr(const std::string &addr) -> void {
            leader_addr = addr;
        }

        auto get_leader_addr(std::string &result) -> void {
            result = leader_addr;
        }

        auto add_to_log(const std::string &cmd) -> void {
            log.emplace_back(cmd);
        }

        auto size_log() -> uint32_t {
            return log.size();
        }

        auto done() -> void {
            ++lastapplied;
        }

        auto prepare_heartbeat(cloud::CloudMessage &hb) -> void {
            hb.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
            hb.set_type(cloud::CloudMessage_Type_REQUEST);
            auto addr = hb.mutable_address();
            addr->set_address(own_addr);
            auto tmp = hb.add_partition();
            tmp->set_id(term());
            tmp->set_peer("");
            for (auto &msg: log) {
                auto tmp1 = hb.add_kvp();
                tmp1->set_key(msg);
                tmp1->set_value("");
            }
        }
        auto prepare_election(cloud::CloudMessage &vt) -> void {
            vt.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);
            vt.set_type(cloud::CloudMessage_Type_REQUEST);
            auto addr = vt.mutable_address();
            addr->set_address(own_addr);
            auto tmp = vt.add_partition();
            tmp->set_id(term());
            tmp->set_peer("");
            tmp = vt.add_partition();
            tmp->set_id(size_log());
            tmp->set_peer("");
        }


    private:
        auto worker(Routing &routing, std::mutex &mtx) -> void;

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
        std::unordered_set<SocketAddress> dropped_peers;
        std::atomic_uint16_t votes_received{0};
        // election timer
        std::chrono::high_resolution_clock::time_point election_timer;
        std::chrono::high_resolution_clock::duration election_timeout_val{};
        // log
        uint32_t lastapplied{};
        std::vector<std::string> log{};


    };

}  // namespace cloudlab

#endif  // CLOUDLAB_RAFT_HH