# Antithesis Test Suite

## How to Dump Artifacts from a Test Run

To dump the stress tool's log and the database from an Antithesis test run, follow these steps. Note that they require
access to the Turso tenant (currently only Turso employees and contractors).

First, start a [multiverse debugging](https://antithesis.com/docs/multiverse_debugging/). In a test run's view, hover
over the last timestamp in the log view, then click it to copy the "moment" details. Then, replace the session id, input
hash and vtime with yours in the request below. The `Authorization` header contains a base64-encoded
`username:password`. The values are in the AWS secret
manager ([link](https://us-east-1.console.aws.amazon.com/secretsmanager/secret?name=antithesis%2Fturso&region=us-east-1)).

```sh
curl -X POST --location "https://turso.antithesis.com/api/v1/launch/debugging" \
    -H "Authorization: Basic XXX" \
    -H "Content-Type: application/json" \
    -d '{
          "params": {
            "antithesis.debugging.session_id": "0c5577383578e9291d87c77424671dd7-44-22",
            "antithesis.debugging.input_hash": "5148272061563518",
            "antithesis.debugging.vtime": "23.747334333136678",
            "antithesis.report.recipients": "you@turso.tech"
          }
        }'
```

After 5 minutes or so, you will receive an email with a link to your session. The left half of the screen is a
JavaScript Jupyter-like notebook that you can use to run commands one at a time. on your test environment.

Paste these commands to print a download link to the log:

```js
container = environment.containers.list({moment})[0].name
print(environment.extract_file({moment, path: "limbostress.log", container}))
```

To get the database, find the log line that looks like this:

```
db_file=/tmp/.tmpUN86Wk
```

Don't forget to get the WAL. If it exists, it will be named something like `.tmpUN86Wk-wal`.

## I have the DB and log, now what?

Sometimes it's enough to feed the SQL to Turso to reproduce the issue. Remove the few lines at the start that are just
numbers, add semicolonsto lines that don't already have one, and redirect the file to `cargo run`, using the same VFS as
the test. Here's a Vim command for to add semicolons to lines that don't already have one:

```
:%s/\([^;]\)$/\1;/
```

If that doesn't do it, then it's going to take more work. You can try observing the state of the B-trees
using [sqlite-viz](https://github.com/LeMikaelF/sqlite-viz). SQLite (and possibly Turso) can mask corruption. sqlite-viz
can also dump information about certain pages in a human-readable form (see the `dump` command and the `-p` flag for
pages, or `-t` for trees), so that you can either reason better about them, or pass them to an LLM.

You might also try binary-searching for the time that the corruption occured. The resolution of `rewind()` is
only one millisecond, but that will at least give you an idea:

```js
// play with this
before = moment.rewind(Time.milliseconds(1000))
// for every timestamp you check, download the DB and its WAL
print(environment.extract_file({moment: before, path: "/tmp/.tmp13REAK", container}))
print(environment.extract_file({moment: before, path: "/tmp/.tmp13REAK-wal", container}))
```
