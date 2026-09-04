import os
import random as _random
import secrets
import struct

# Per-process RNG; antithesis-style entropy from the OS.
_rng = _random.Random(secrets.randbits(64))


def get_random() -> int:
    return _rng.getrandbits(63)


def random_choice(seq):
    return _rng.choice(seq)
