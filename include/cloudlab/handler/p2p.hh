#ifndef CLOUDLAB_P2P_HH
#define CLOUDLAB_P2P_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/kvs.hh"
#include "cloudlab/network/routing.hh"
#include "cloudlab/raft/raft.hh"

namespace cloudlab {

/**
 * Handler for P2P requests. Takes care of the messages from peers, cluster
 * metadata / routing tier, and the API.
 */
class P2PHandler : public ServerHandler {
 public:
  explicit P2PHandler(Routing& routing);

  auto handle_connection(Connection& con) -> void override;

  auto set_raft_leader() -> void {
    raft->set_leader();
  }

  auto set_raft_candidate() -> void {
    raft->set_candidate();
  }

  auto set_raft_follower() -> void {
    raft->set_follower();
  }

  auto get_raft_role() -> RaftRole {
    return raft->get_role();
  }

  auto raft_run() -> std::thread {
    return raft->run(routing, mtx);
  }

 private:
  // clang-format off
  auto handle_put(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_get(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_delete(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_key_operation_leader(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_join_cluster(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_create_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_steal_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_drop_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_transfer_partition(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_raft_append_entries(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_raft_vote(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_raft_dropped_node(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_raft_get_leader(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_raft_direct_get(Connection& con, const cloud::CloudMessage& msg) -> void;
  // clang-format on

  std::unordered_map<uint32_t, std::unique_ptr<KVS>> partitions{};

  std::unique_ptr<Raft> raft;
  Routing& routing;
  std::mutex mtx;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_P2P_HH
