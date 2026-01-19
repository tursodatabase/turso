#!/usr/bin/env bash

/bin/turso_stress --silent --nr-threads 2 --nr-iterations 10000 --vfs io_uring
