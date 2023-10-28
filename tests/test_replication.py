#!/usr/bin/env python3

import sys, random, os, signal
from time import sleep
from testsupport import subtest, run
from socketsupport import run_leader, run_kvs, run_ctl

def kill_nodes(nodes) -> None:
    for i in range(len(nodes)):
        run(["kill", "-9", str(nodes[i][0].pid)])

def main() -> None:
    with subtest("Testing replication"):
        leader = run_leader("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2    = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")
        kvs3    = run_kvs("127.0.0.1:46000", "127.0.0.1:47000", "127.0.0.1:41000")
        kvs_list = [[leader, "127.0.0.1:40000", "127.0.0.1:41000"],
                    [kvs1, "127.0.0.1:42000", "127.0.0.1:43000"],
                    [kvs2, "127.0.0.1:44000", "127.0.0.1:45000"],
                    [kvs3, "127.0.0.1:46000", "127.0.0.1:47000"]]
        sleep(2)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            kill_nodes(kvs_list[:-1])
            sys.exit(1)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            kill_nodes(kvs_list[:-1])
            sys.exit(1)

        sleep(5)
        
        keys = random.sample(range(1, 21), 20)
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "put", f"{k} 2")
            if "OK" not in ctl:
                kill_nodes(kvs_list[:-1])
                sys.exit(1)
        
        sleep(5)
        # sys.exit(0)
        for i in range(len(kvs_list) - 1):
            for k in keys:
                ctl = run_ctl(kvs_list[i][1], "direct_get", f"{k}")
                if "Value:\t2" not in ctl:
                    kill_nodes(kvs_list[:-1])
                    print("Failing first subtest")
                    sys.exit(1)
        
        print("Passing first subtest")
        print("Test successful.")

        # Add new kvs
        sleep(1)
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:47000")
        if "OK" not in ctl:
            kill_nodes(kvs_list)
            sys.exit(1)
        
        sleep(5)

        for k in keys:
            ctl = run_ctl(kvs_list[-1][1], "direct_get", f"{k}")
            if "Value:\t2" not in ctl:
                kill_nodes(kvs_list)
                print("Failing second subtest")
                sys.exit(1)

        print("Passing second subtest")
        kill_nodes(kvs_list)
        print("Test successful.")
        sys.exit(0)

if __name__ == "__main__":
    main()