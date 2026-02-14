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
            # Retry with different ports in case the chosen port is
            # unavailable (common on Windows where OS reserves port ranges).
            max_attempts = 5
            for attempt in range(max_attempts):
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
                deadline = time.time() + 30
                while time.time() < deadline:
                    rc = self._server.poll()
                    if rc is not None:
                        stderr = self._server.stderr.read().decode(errors="replace")
                        if "os error 10013" in stderr or "address already in use" in stderr.lower():
                            break  # retry with a different port
                        raise RuntimeError(
                            f"sync server exited with code {rc} before accepting connections\nstderr: {stderr}"
                        )
                    try:
                        requests.get(self._user_url, timeout=5)
                        break
                    except Exception:
                        time.sleep(0.1)
                else:
                    stderr = ""
                    if self._server.poll() is not None:
                        stderr = self._server.stderr.read().decode(errors="replace")
                    self._server.kill()
                    raise TimeoutError(
                        f"sync server did not become available within 30s\nstderr: {stderr}"
                    )
                # If the inner loop broke out due to a port conflict, retry
                if self._server.poll() is not None:
                    if attempt == max_attempts - 1:
                        raise RuntimeError(
                            f"sync server failed to bind after {max_attempts} port attempts"
                        )
                    continue
                break  # server is up

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
