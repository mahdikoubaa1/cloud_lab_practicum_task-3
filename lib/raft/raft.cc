#include "cloudlab/raft/raft.hh"


namespace cloudlab {


auto Raft::put(const std::string& key, const std::string& value) -> bool {
  // TODO(you)
}

auto Raft::perform_election(Routing& routing) -> void {
  // TODO(you)
  // Upon election timeout, the follower changes to candidate and starts election  
}

auto Raft::heartbeat(Routing& routing, std::mutex& mtx) -> void {
  // TODO(you)
  // Implement the heartbeat functionality that the leader should broadcast to 
  // the followers to declare its presence
}

auto Raft::run(Routing& routing, std::mutex& mtx) -> std::thread {
  // TODO(you)
  // Return a thread that keeps running the heartbeat function.
  // If you have other implementation you can skip this.   

  return {};
}


}  // namespace cloudlab