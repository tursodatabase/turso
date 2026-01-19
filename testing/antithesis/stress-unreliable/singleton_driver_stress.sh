#!/usr/bin/env bash

LD_PRELOAD=/usr/lib/unreliable-libc.so /bin/turso_stress --silent --nr-iterations 10000
