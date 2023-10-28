#!/usr/bin/env python3

import sys, random
from time import sleep
from testsupport import subtest, run
from socketsupport import run_leader, run_kvs, run_ctl

def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i][0].pid)])

def main() -> None:
    with subtest("Testing leader election"):
        leader = run_leader("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2    = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")
        kvs3    = run_kvs("127.0.0.1:46000", "127.0.0.1:47000", "127.0.0.1:41000")
        kvs4    = run_kvs("127.0.0.1:48000", "127.0.0.1:49000", "127.0.0.1:41000")
        kvs_list = [[kvs1, "127.0.0.1:42000", "127.0.0.1:43000"], 
                    [kvs2, "127.0.0.1:44000", "127.0.0.1:45000"], 
                    [kvs3, "127.0.0.1:46000", "127.0.0.1:47000"],
                    [kvs4, "127.0.0.1:48000", "127.0.0.1:49000"]]
        ctl_list = []
        sleep(2)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            kill_nodes(kvs_list)
            sys.exit(1)
        
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            kill_nodes(kvs_list)
            sys.exit(1)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:47000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            kill_nodes(kvs_list)
            sys.exit(1)
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:49000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            kill_nodes(kvs_list)
            sys.exit(1)
        sleep(2)
        
        run(["kill", "-9", str(leader.pid)])
        sleep(5)

        ctl_1 = run_ctl("127.0.0.1:42000", "leader")
        ctl_2 = run_ctl("127.0.0.1:44000", "leader")
        ctl_3 = run_ctl("127.0.0.1:46000", "leader")
        ctl_4 = run_ctl("127.0.0.1:48000", "leader")

        if ctl_1 == "127.0.0.1:41000" or (not ctl_1 == ctl_2 == ctl_3 == ctl_4):
            kill_nodes(kvs_list)
            print("Failing first subtest")
            sys.exit(1)

        print("Passing first subtest")
        ctl_1 = ctl_1.strip()

        for i in range(len(kvs_list)):
            if ctl_1 == kvs_list[i][2]:
                leader = kvs_list.pop(i)
                break
        
        run(["kill", "-9", str(leader[0].pid)])
        print("Killing leader " + leader[2])
        sleep(5)

        for i in range(len(kvs_list)):
            ctl_list.append(run_ctl(kvs_list[i][1], "leader"))

        ctl_list[0] = ctl_list[0].strip()
        ctl_list[1] = ctl_list[1].strip()
        ctl_list[2] = ctl_list[2].strip()

        if (ctl_list[0] == leader[2]) or (not ctl_list[0] == ctl_list[1] == ctl_list[2]):
            kill_nodes(kvs_list)
            print("failing second subtest")
            sys.exit(1)

        print("Passing second subtest")

        kill_nodes(kvs_list)
        print("Test successful.")
        sys.exit(0)

if __name__ == "__main__":
    main()