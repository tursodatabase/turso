# Django's tests/test_sqlite.py plus: the backend picked by ORM_DRIVER, on-disk
# test databases (turso does not support sqlite3 URI filenames such as
# "file:memorydb_default?mode=memory&cache=shared"), and JUnit XML output.
import os

_engine = {"sqlite": "django.db.backends.sqlite3", "turso": "turso_backend"}[os.environ["ORM_DRIVER"]]
_dbdir = os.environ["ORM_DB_DIR"]

DATABASES = {
    alias: {"ENGINE": _engine, "TEST": {"NAME": os.path.join(_dbdir, f"{alias}.db")}}
    for alias in ("default", "other")
}

SECRET_KEY = "django_tests_secret_key"
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
USE_TZ = False

TEST_RUNNER = "xmlrunner.extra.djangotestrunner.XMLTestRunner"
TEST_OUTPUT_DIR = os.environ["ORM_RESULTS_DIR"]
TEST_OUTPUT_FILE_NAME = os.environ["ORM_XML_NAME"]
