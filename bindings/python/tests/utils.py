import os
import random
import string
import subprocess
import time

import requests


def random_str() -> str:
    return "".join([random.choice(string.ascii_letters) for _ in range(8)])


def handle_response(r):
    if r.status_code == 400 and "already exists" in r.text:
        return
    r.raise_for_status()


ADMIN_URL = "http://localhost:8081"
USER_URL = "http://localhost:8080"


class TursoServer:
    def __init__(self):
        if "LOCAL_SYNC_SERVER" not in os.environ:
            name = random_str()
            tokens = USER_URL.split("://")
            handle_response(requests.post(ADMIN_URL + f"/v1/tenants/{name}"))
            handle_response(requests.post(ADMIN_URL + f"/v1/tenants/{name}/groups/{name}"))
            handle_response(requests.post(ADMIN_URL + f"/v1/tenants/{name}/groups/{name}/databases/{name}"))
            self._user_url = USER_URL
            self._db_url = f"{tokens[0]}://{name}--{name}--{name}.{tokens[1]}"
            self._host = f"{name}--{name}--{name}.localhost"
            self._server = None
        else:
            port = random.randint(10_000, 65535)
            self._server = subprocess.Popen(
                [os.environ["LOCAL_SYNC_SERVER"], "--sync-server", f"0.0.0.0:{port}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._user_url = f"http://localhost:{port}"
            self._db_url = f"http://localhost:{port}"
            self._host = ""
            # wait for server to be available
            while True:
                try:
                    requests.get(self._user_url)
                    break
                except Exception:
                    time.sleep(0.1)
            return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._server:
            self._server.kill()

    def db_url(self) -> str:
        return self._db_url

    def db_sql(self, sql: str):
        result = requests.post(
            self._user_url + "/v2/pipeline",
            json={"requests": [{"type": "execute", "stmt": {"sql": sql}}]},
            headers={"Host": self._host},
        )
        result.raise_for_status()
        result = result.json()
        if result["results"][0]["type"] != "ok":
            raise Exception(f"remote sql execution failed: {result}")
        return [[cell["value"] for cell in row] for row in result["results"][0]["response"]["result"]["rows"]]
