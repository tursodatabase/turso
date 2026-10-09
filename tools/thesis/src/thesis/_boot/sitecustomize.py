"""Seeds Python's global RNG in every process that thesis launches.

Outside Antithesis, the SDK's get_random() and random_choice() fall back to
random.getrandbits(), so seeding the global RNG before the driver starts
makes every random decision the driver takes reproducible.

thesis puts this directory at the front of PYTHONPATH. Afterwards we run any
sitecustomize module that ours shadowed, so the environment is otherwise
unchanged.
"""

import os
import random
import sys


def _seed() -> None:
    seed = os.environ.get("THESIS_SEED")
    if seed is not None:
        random.seed(int(seed))


def _chain() -> None:
    import importlib.machinery
    import importlib.util

    here = os.path.dirname(os.path.abspath(__file__))
    rest = [p for p in sys.path if os.path.abspath(p or os.curdir) != here]
    spec = importlib.machinery.PathFinder.find_spec("sitecustomize", rest)
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


_seed()
_chain()
