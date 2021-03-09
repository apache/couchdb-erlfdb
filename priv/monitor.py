#!/usr/bin/env python3

import os
import sys
import time


def main():
    parent_pid = int(sys.argv[1])
    target_pid = int(sys.argv[2])
    while True:
        try:
            os.kill(parent_pid, 0)
            time.sleep(1.0)
        except OSError:
            try:
                os.kill(target_pid, 9)
            except:
                pass
            exit(0)


if __name__ == "__main__":
    main()
