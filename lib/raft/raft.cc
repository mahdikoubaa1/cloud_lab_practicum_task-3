#include <condition_variable>
#include "cloudlab/raft/raft.hh"


namespace cloudlab {


    auto Raft::put(const std::string &key, const std::string &value) -> bool {
        return kvs.put(key, value);
    }

    auto Raft::perform_election(Routing &routing, std::mutex &mtx) -> void {
        reset_election_timer();
        std::mutex mtx1;
        std::condition_variable cv;
        std::mutex mtx2;
        std::unique_lock<std::mutex> l1(mtx2);
        std::mutex mtx3;
        std::unique_lock<std::mutex> l2(mtx3);
        l1.unlock();
        l2.unlock();
        std::condition_variable cv1;
        auto timer = election_timer;
        bool timeout;
        bool timeout1;
        std::unique_lock<std::mutex> l(mtx1);

        std::unordered_set<SocketAddress> responded;

        while (candidate()) {
            timeout = false;
            timeout1 = false;
            std::thread t([&l1, &timer, &cv, &cv1, &timeout]() {
                l1.lock();
                cv1.wait_until(l1, timer);
                timeout = true;
                l1.unlock();
                cv.notify_all();

            });
            std::thread t1([&l2, &cv, &cv1, &timeout1]() {
                l2.lock();
                cv1.wait_for(l2, std::chrono::milliseconds(450));
                timeout1 = true;
                l2.unlock();
                cv.notify_all();
            });
            cloud::CloudMessage vt;
            prepare_election(vt);
            std::vector<std::pair<SocketAddress, std::unique_ptr<Connection>>> connections;
            int i = 0;
            auto peers = routing.partitions_by_peer();
            for (auto &peer: peers) {
                if (!responded.contains(peer.first)) {
                    connections.emplace_back(peer.first, std::make_unique<Connection>(peer.first));
                    if (connections.at(i).second->connect_failed || !connections.at(i).second->send(vt)) {
                        dropped_peers.emplace(peer.first);
                    } else {
                        auto todelete = dropped_peers.find(peer.first);
                        if (todelete != dropped_peers.end())dropped_peers.erase(todelete);
                    }
                    ++i;
                }
            }
            i = 0;
            for (auto &con: connections) {
                if (dropped_peers.contains(con.first)) {
                    ++i;
                    continue;
                }
                mtx.unlock();
                bool b = connections.at(i).second->receive(vt);
                mtx.lock();
                if (follower()) {
                    cv1.notify_all();
                    try {
                        t.join();
                    } catch (std::system_error &e) {

                    }
                    try {
                        t1.join();
                    } catch (std::system_error &e) {

                    }
                    return; }
                if (election_timeout()) break;
                if (!b) {
                    dropped_peers.emplace(con.first);
                    ++i;
                    continue;
                }
                responded.emplace(con.first);
                if (vt.success() && current_term == vt.partition(0).id() &&
                    ++votes_received > ((peers.size() + 1) / 2) &&
                    !election_timeout()) {
                    set_leader();
                    cv1.notify_all();
                    try {
                        t.join();
                    } catch (std::system_error &e) {

                    }
                    try {
                        t1.join();
                    } catch (std::system_error &e) {

                    }
                    return;

                } else if (!vt.success() && current_term < vt.partition(0).id()) {
                    set_follower();
                    set_term(vt.partition(0).id());
                    cv1.notify_all();
                    try {
                        t.join();
                    } catch (std::system_error &e) {

                    }
                    try {
                        t1.join();
                    } catch (std::system_error &e) {

                    }
                    return;
                }
                ++i;
            }
            if (election_timeout()) {
                set_candidate();
                ++current_term;
                votes_received = 1;
                voted_for = SocketAddress(own_addr);
                cv1.notify_all();
                try {
                    t.join();
                } catch (std::system_error &e) {

                }
                try {
                    t1.join();
                } catch (std::system_error &e) {

                }
                responded.clear();
                reset_election_timer();
                return;
            }
            mtx.unlock();
            cv.wait_for(l, std::chrono::milliseconds(450), [&timeout, &timeout1] { return timeout || timeout1; });
            mtx.lock();
            cv1.notify_all();
            try {
                t.join();
            } catch (std::system_error &e) {

            }
            try {
                t1.join();
            } catch (std::system_error &e) {

            }
            if (election_timeout()) {
                set_candidate();
                ++current_term;
                votes_received = 1;
                voted_for = SocketAddress(own_addr);
                responded.clear();
                reset_election_timer();
            }
        }
        // Upon election timeout, the follower changes to candidate and starts election
    }

    auto Raft::heartbeat(Routing &routing, std::mutex &mtx) -> void {
        std::mutex mtx1;
        std::condition_variable cv;
        std::mutex mtx2;
        std::condition_variable cv1;
        std::unique_lock<std::mutex> l(mtx1);
        std::unique_lock<std::mutex> l1(mtx2);
        bool timeout;
        while (leader()) {
            timeout = false;
            std::thread t([&l1, &cv, &cv1, &timeout]() {
                cv1.wait_for(l1, std::chrono::milliseconds(450));
                timeout = true;
                cv.notify_all();
            });
            auto peers = routing.partitions_by_peer();
            cloud::CloudMessage hb;
            prepare_heartbeat(hb);
            std::vector<std::pair<SocketAddress, std::unique_ptr<Connection>>> connections;
            int i = 0;
            for (auto &peer: peers) {
                connections.emplace_back(peer.first, std::make_unique<Connection>(SocketAddress(peer.first)));
                if (connections.at(i).second->connect_failed || !connections.at(i).second->send(hb)) {
                    dropped_peers.emplace(peer.first);
                } else {
                    auto todelete = dropped_peers.find(peer.first);
                    if (todelete != dropped_peers.end())dropped_peers.erase(todelete);
                }
                ++i;
            }
            i = 0;
            for (auto &con: connections) {
                if (dropped_peers.contains(con.first)) {
                    ++i;
                    continue;
                }
                mtx.unlock();
                bool b = connections.at(i).second->receive(hb);
                mtx.lock();
                if (!leader()) {
                    cv1.notify_all();
                    try {
                        t.join();
                    } catch (std::system_error &e) {

                    }
                    if (election_timeout()) {
                        set_candidate();
                        votes_received = 1;
                        voted_for = SocketAddress(own_addr);
                        ++current_term;
                    }

                    return;
                }
                if (!b) {
                    dropped_peers.emplace(con.first);
                    ++i;
                    continue;
                }
                if (!hb.success()) {
                    set_follower();
                    if (current_term < hb.partition(0).id()) set_term(hb.partition(0).id());
                    cv1.notify_all();
                    try {
                        t.join();
                    } catch (std::system_error &e) {

                    }
                    return;
                }
                ++i;
            }
            mtx.unlock();
            cv.wait_for(l, std::chrono::milliseconds(450), [&timeout] { return timeout; });
            mtx.lock();
            try {
                t.join();
            } catch (std::system_error &e) {

            }
            if (!leader() && election_timeout()) {
                set_candidate();
                votes_received = 1;
                voted_for = SocketAddress(own_addr);
                ++current_term;
            }

        }
        // Implement the heartbeat functionality that the leader should broadcast to
        // the followers to declare its presence
    }

    auto Raft::run(Routing &routing, std::mutex &mtx) -> std::thread {
        auto thread = std::thread(&Raft::worker, (this), std::ref(routing), std::ref(mtx));
        // Return a thread that keeps running the heartbeat function.
        // If you have other implementation you can skip this.
        return thread;
    }

    auto Raft::worker(Routing &routing, std::mutex &mtx) -> void {
        while (true) {
            mtx.lock();
            switch (role) {
                case RaftRole::LEADER : {
                    heartbeat(routing, mtx);
                    break;
                }
                case RaftRole::CANDIDATE : {
                    perform_election(routing, mtx);
                    break;
                }
                case RaftRole::FOLLOWER : {
                    reset_election_timer();
                    while (follower()) {
                        mtx.unlock();
                        std::this_thread::sleep_until(election_timer);
                        mtx.lock();
                        if (!leader() && election_timeout()) {
                            set_candidate();
                            votes_received = 1;
                            voted_for = SocketAddress(own_addr);
                            ++current_term;
                            break;
                        }
                    }
                }
                default : {
                    break;
                }
            }
            mtx.unlock();
        }

    }


}  // namespace cloudlab