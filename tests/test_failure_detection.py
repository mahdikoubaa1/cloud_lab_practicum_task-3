#!/usr/bin/env python3

import sys, random, os, signal
from time import sleep
from testsupport import subtest, run
from socketsupport import run_leader, run_kvs, run_ctl

def main() -> None:
    with subtest("Testing failure detection"):
        leader = run_leader("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2    = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")
        kvs_list = [[kvs1, "127.0.0.1:43000"], [kvs2, "127.0.0.1:45000"]]
        sleep(2)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            run(["kill", "-9", str(leader.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        sleep(2)
        ctl = run_ctl("127.0.0.1:40000", "dropped", "")
        
        index = random.randint(0, 1)

        run(["kill", "-9", str(kvs_list[index][0].pid)])
        sleep(2)
        ctl = run_ctl("127.0.0.1:40000", "dropped", "")

        if kvs_list[index][1] not in ctl:
            run(["kill", "-9", str(leader.pid)])
            run(["kill", "-9", str(kvs_list[1-index][0].pid)])
            print("failing first subtest")
            sys.exit(1)

        print("Passing first subtest")
        
        run(["kill", "-9", str(kvs_list[1-index][0].pid)])
        sleep(2)
        ctl = run_ctl("127.0.0.1:40000", "dropped")

        if kvs_list[1-index][1] not in ctl:
            run(["kill", "-9", str(leader.pid)])
            print("failing second subtest")
            sys.exit(1)

        run(["kill", "-9", str(leader.pid)])
        print("Test successful.")
        sys.exit(0)
        


if __name__ == "__main__":
    main()