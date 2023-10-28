#include "cloudlab/handler/p2p.hh"
#include <condition_variable>

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {

    P2PHandler::P2PHandler(Routing &routing) : routing{routing} {
        auto hash = std::hash<SocketAddress>()(routing.get_backend_address());
        auto path = fmt::format("/tmp/{}-initial", hash);
        auto raft_path = fmt::format("/tmp/{}-raft", hash);
        partitions.insert({0, std::make_unique<KVS>(path)});
        raft = std::make_unique<Raft>(raft_path, routing.get_backend_address().string());
    }

    auto P2PHandler::handle_connection(Connection &con) -> void {
        cloud::CloudMessage request{}, response{};

        if (!con.receive(request)) {
            return;
        }

        switch (request.operation()) {
            case cloud::CloudMessage_Operation_PUT: {
                if (raft->leader()) {
                    handle_key_operation_leader(con, request);
                } else {
                    handle_put(con, request);
                }
                break;
            }
            case cloud::CloudMessage_Operation_GET: {
                if (raft->leader()) {
                    handle_key_operation_leader(con, request);
                } else {
                    handle_get(con, request);
                }
                break;
            }
            case cloud::CloudMessage_Operation_DELETE: {
                if (raft->leader()) {
                    handle_key_operation_leader(con, request);
                } else {
                    handle_delete(con, request);
                }
                break;
            }
            case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
                handle_join_cluster(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_CREATE_PARTITIONS: {
                handle_create_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_STEAL_PARTITIONS: {
                handle_steal_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_DROP_PARTITIONS: {
                handle_drop_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_TRANSFER_PARTITION: {
                handle_transfer_partition(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES: {
                handle_raft_append_entries(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_RAFT_VOTE: {
                handle_raft_vote(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE: {
                handle_raft_dropped_node(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_RAFT_GET_LEADER: {
                handle_raft_get_leader(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_RAFT_DIRECT_GET: {
                handle_raft_direct_get(con, request);
                break;
            }
            default:
                response.set_type(cloud::CloudMessage_Type_RESPONSE);
                response.set_operation(request.operation());
                response.set_success(false);
                response.set_message("Operation not (yet) supported");

                con.send(response);

                break;
        }
    }

    auto P2PHandler::handle_put(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_PUT);
        std::string tmp;
        raft->get_leader_addr(tmp);
        auto leaderaddress = response.mutable_address();
        leaderaddress->set_address(tmp);
        response.set_success(false);
        response.set_message("ERROR");
        con.send(response);
    }

    auto P2PHandler::handle_get(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_GET);
        std::string tmp;
        raft->get_leader_addr(tmp);
        auto leaderaddress = response.mutable_address();
        leaderaddress->set_address(tmp);
        response.set_success(false);
        response.set_message("ERROR");
        con.send(response);
    }

    auto P2PHandler::handle_delete(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_DELETE);
        std::string tmp;
        raft->get_leader_addr(tmp);
        auto leaderaddress = response.mutable_address();
        leaderaddress->set_address(tmp);
        response.set_success(false);
        response.set_message("ERROR");

        con.send(response);
    }

    auto P2PHandler::handle_key_operation_leader(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_operation(msg.operation());
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        mtx.lock();
        if (!raft->leader()) {
            response.set_success(false);
            response.set_message("ERROR");
            std::string tmp;
            raft->get_leader_addr(tmp);
            auto leaderaddress = response.mutable_address();
            leaderaddress->set_address(tmp);
        } else {
            response.set_success(true);
            response.set_message("OK");
            raft->add_to_log(msg.SerializeAsString());
            switch (msg.operation()) {
                case cloud::CloudMessage_Operation_GET: {
                    std::string value;
                    for (const auto &kvp: msg.kvp()) {

                        auto *tmp = response.add_kvp();
                        tmp->set_key(kvp.key());
                        if (raft->get(kvp.key(), value)) {
                            tmp->set_value(value);
                        } else {
                            tmp->set_value("ERROR");
                        }
                    }
                    break;
                }
                case cloud::CloudMessage_Operation_PUT: {
                    for (const auto &kvp: msg.kvp()) {
                        auto *tmp = response.add_kvp();
                        tmp->set_key(kvp.key());
                        if (raft->put(kvp.key(), kvp.value())) {
                            tmp->set_value("OK");
                        } else {
                            tmp->set_value("ERROR");
                            response.set_success(false);
                            response.set_message("ERROR");
                        }
                    }
                    break;
                }
                case cloud::CloudMessage_Operation_DELETE: {
                    for (const auto &kvp: msg.kvp()) {
                        auto *tmp = response.add_kvp();
                        tmp->set_key(kvp.key());
                        if (raft->remove(kvp.key())) {
                            tmp->set_value("OK");
                        } else {
                            tmp->set_value("ERROR");
                        }
                    }
                    break;
                }
                default: {
                    break;
                }
            }
        }
        // This function should be similar to the RouterHandler::handle_key_operation()
        // in task 2.
        mtx.unlock();
        con.send(response);
    }

    auto P2PHandler::handle_join_cluster(Connection &con,
                                         const cloud::CloudMessage &msg) -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
        mtx.lock();
        switch (msg.type()) {
            case cloud::CloudMessage_Type_NOTIFICATION : {
                response.set_message("OK");
                response.set_success(true);

                for (const auto &kvp: msg.kvp()) {
                    if (kvp.value() != routing.get_backend_address().string())
                        routing.add_peer(0, SocketAddress(
                                kvp.value()));
                }
                std::string leader_addr;
                raft->get_leader_addr(leader_addr);
                if (leader_addr.empty()) {
                    routing.set_cluster_address(SocketAddress(msg.address().address()));
                    raft->set_leader_addr(msg.address().address());
                    raft->set_follower();
                    raft->set_term(msg.partition(0).id());
                    raft->reset_election_timer();

                }
                break;
            }
            case cloud::CloudMessage_Type_REQUEST : {
                response.set_message("OK");
                response.set_success(true);
                routing.add_peer(0, SocketAddress(msg.address().address()));
                cloud::CloudMessage notif;
                notif.set_type(cloud::CloudMessage_Type_NOTIFICATION);
                notif.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
                auto addr = notif.mutable_address();
                std::string la;
                raft->get_leader_addr(la);
                addr->set_address(la);
                auto peers = routing.partitions_by_peer();
                for (auto &peer: peers) {
                    auto tmp = notif.add_kvp();
                    tmp->set_key("");
                    tmp->set_value(peer.first.string());
                }
                auto tmp = notif.add_kvp();
                tmp->set_key("");
                tmp->set_value(routing.get_backend_address().string());
                auto tmp1 = notif.add_partition();
                tmp1->set_id(raft->term());
                std::vector<std::pair<SocketAddress, std::unique_ptr<Connection>>> connections;
                int i = 0;
                for (auto &peer: peers) {
                    connections.emplace_back(peer.first, std::make_unique<Connection>(SocketAddress(peer.first)));
                    connections.at(i).second->send(notif);
                    ++i;
                }
                i = 0;
                for (auto &con2: connections) {
                    if (!con2.second->connect_failed) {
                        con2.second->receive(notif);
                    }
                    ++i;
                }
                break;
            }
            default: {
                break;
            }
        }

        // Handle join cluster request. Leader might operate differently from followers.
        mtx.unlock();
        con.send(response);
    }

    auto P2PHandler::handle_create_partitions(Connection &con,
                                              const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        // TODO from the 2nd task - not required

        con.send(response);
    }

    auto P2PHandler::handle_steal_partitions(Connection &con,
                                             const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        // TODO from the 2nd task - not required

        con.send(response);
    }

    auto P2PHandler::handle_drop_partitions(Connection &con,
                                            const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        // TODO from the 2nd task - not required

        con.send(response);
    }

    auto P2PHandler::handle_transfer_partition(Connection &con,
                                               const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};

        // TODO from the 2nd task - not required

        con.send(response);
    }

    auto P2PHandler::handle_raft_append_entries(Connection &con,
                                                const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
        mtx.lock();
        auto currentterm = raft->term();

        uint32_t size = raft->size_log();
        cloud::CloudMessage msg1;
        if (currentterm <= msg.partition(0).id()) {
            raft->set_term(msg.partition(0).id());
            if (msg.kvp().size() >= size) {
                response.set_success(true);
                response.set_message("OK");
                raft->set_follower();
                raft->set_leader_addr(msg.address().address());
                routing.set_cluster_address(SocketAddress(msg.address().address()));
                int i = 0;
                for (const auto &kvp: msg.kvp()) {
                    if (i < size) {
                        ++i;
                        continue;
                    }
                    raft->add_to_log(kvp.key());
                    msg1.ParseFromString(kvp.key());
                    switch (msg1.operation()) {
                        case cloud::CloudMessage_Operation_PUT: {
                            for (const auto &kvp1: msg1.kvp()) {
                                raft->put(kvp1.key(), kvp1.value());
                            }
                            break;
                        }
                        case cloud::CloudMessage_Operation_DELETE: {
                            for (const auto &kvp1: msg1.kvp()) {
                                raft->remove(kvp1.key());
                            }
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
                raft->reset_election_timer();
            } else {
                response.set_success(false);
                response.set_message("ERROR");
            }
        } else {
            response.set_success(false);
            response.set_message("ERROR");
        }
        auto tmp = response.add_partition();
        tmp->set_id(raft->term());
        tmp->set_peer("");
        // Do things when receiving heartbeat from the leader.
        mtx.unlock();

        con.send(response);
    }

    auto P2PHandler::handle_raft_vote(Connection &con,
                                      const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);
        mtx.lock();
        auto currentterm = raft->term();
        if (currentterm < msg.partition(0).id() && raft->size_log() <= msg.partition(1).id()) {
            response.set_success(true);
            response.set_message("OK");
            raft->set_voted_for(SocketAddress(msg.address().address()));
            raft->set_term(msg.partition(0).id());
            raft->set_follower();
            routing.set_cluster_address(SocketAddress(msg.address().address()));
            raft->reset_election_timer();

        } else {
            response.set_success(false);
            response.set_message("ERROR");
        }
        auto tmp = response.add_partition();
        tmp->set_id(raft->term());
        tmp->set_peer("");
        // Decide whether to vote for the sender.
        mtx.unlock();
        con.send(response);
    }

    auto P2PHandler::handle_raft_dropped_node(Connection &con,
                                              const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_RAFT_DROPPED_NODE);
        mtx.lock();
        if (raft->leader()) {
            std::vector<std::string> dropped;
            raft->get_dropped_peers(dropped);
            for (auto &peer: dropped) {
                auto tmp = response.add_kvp();
                tmp->set_key("");
                tmp->set_value(peer);
            }
            response.set_success(true);
            response.set_message("OK");
        } else {
            auto tmp = response.add_kvp();
            tmp->set_key("I am not leader");
            tmp->set_value("peer");
            response.set_success(false);
            response.set_message("ERROR");
        }
        // Return the address of dropped nodes
        mtx.unlock();
        con.send(response);
    }

    auto P2PHandler::handle_raft_get_leader(Connection &con,
                                            const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_RAFT_GET_LEADER);
        std::string tmp;
        mtx.lock();
        raft->get_leader_addr(tmp);
        response.set_message(tmp);
        response.set_success(true);
        mtx.unlock();
        con.send(response);
    }

    auto P2PHandler::handle_raft_direct_get(Connection &con,
                                            const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_operation(cloud::CloudMessage_Operation_RAFT_DIRECT_GET);
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_success(true);
        response.set_message("OK");
        std::string value;
        for (const auto &kvp: msg.kvp()) {
            auto *tmp = response.add_kvp();
            tmp->set_key(kvp.key());
            if (raft->get(kvp.key(), value)) {
                tmp->set_value(value);
            } else {
                tmp->set_value("ERROR");
            }
        }
        std::cout << response.DebugString();
        // Return the get request from clt directly, regardless if you are leader
        // or not.
        con.send(response);
    }


}  // namespace cloudlab