/*
 * Turso + libuv: driving async IO from a non-Rust event loop.
 *
 * Turso's C ABI can run in `async_io` mode, where an operation that would
 * normally block returns TURSO_IO instead. It is then the *caller's* job to
 * decide when to make progress, by calling turso_statement_run_io() and
 * retrying. That is the hook an external I/O loop needs: this example wires
 * Turso to libuv so that all database work happens inside libuv callbacks and
 * the loop decides the pacing.
 *
 * Note the integration point this example uses. Turso has no libuv VFS (see
 * `vfs` in sdk-kit/turso.h: "memory", "syscall", "io_uring",
 * "experimental_win_iocp"), and turso_statement_run_io() is the only way to
 * pump the I/O backend - there is no database- or connection-level equivalent.
 * So with async_io the file I/O still happens inside Turso; what libuv takes
 * over is *scheduling*: which callback runs when, how many statements are in
 * flight, and how that work is interleaved with the rest of the loop. That is
 * the integration path this example demonstrates - a non-Rust event loop in
 * the driver's seat - without Turso needing to know about libuv at all.
 *
 * Open and connect are done once, before the loop starts, and are the two
 * calls that may report TURSO_IO without a statement to pump it with; the
 * example checks for that and reports it rather than silently ignoring it.
 *
 * Build: see the Makefile in this directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "turso.h"
#include <uv.h>

static const char *g_err = NULL;

static void die(const char *what, const char *err)
{
    fprintf(stderr, "%s: %s\n", what, err ? err : "no message");
    exit(1);
}

/*
 * Drive a statement to completion, pumping the I/O backend on every TURSO_IO.
 * Returns the final status (TURSO_DONE, TURSO_ROW or an error code). This is
 * the one place the example talks to the IO backend, so every libuv callback
 * that touches the database goes through it.
 */
static turso_status_code_t run_statement(turso_statement_t *stmt, int stop_on_row)
{
    for (;;) {
        const char *err = NULL;
        turso_status_code_t rc = stop_on_row ? turso_statement_step(stmt, &err)
                                             : turso_statement_execute(stmt, NULL, &err);
        if (rc != TURSO_IO) {
            g_err = err;
            return rc;
        }
        /* One iteration of the I/O backend, then let the caller retry. */
        if (turso_statement_run_io(stmt, &err) != TURSO_OK) {
            g_err = err;
            return TURSO_IOERR;
        }
    }
}

static void print_row(const turso_statement_t *stmt)
{
    int cols = (int)turso_statement_column_count(stmt);
    for (int i = 0; i < cols; i++) {
        switch (turso_statement_row_value_kind(stmt, i)) {
        case TURSO_TYPE_INTEGER:
            printf("%lld", (long long)turso_statement_row_value_int(stmt, i));
            break;
        case TURSO_TYPE_REAL:
            printf("%.2f", turso_statement_row_value_double(stmt, i));
            break;
        case TURSO_TYPE_TEXT: {
            const char *text = turso_statement_row_value_bytes_ptr(stmt, i);
            int len = (int)turso_statement_row_value_bytes_count(stmt, i);
            printf("%.*s", len, text ? text : "");
            break;
        }
        case TURSO_TYPE_NULL:
            printf("NULL");
            break;
        default:
            printf("<blob>");
            break;
        }
        if (i + 1 < cols) {
            printf(" | ");
        }
    }
    printf("\n");
}

typedef struct {
    uv_loop_t *loop;
    const turso_connection_t *conn; /* the API takes it as const everywhere */
    uv_timer_t tick;
    int ticks;
} app_t;

/* Prepare, run to completion and finalize a statement that returns no rows. */
static void exec_sql(app_t *app, const char *sql)
{
    const char *err = NULL;
    turso_statement_t *stmt = NULL;
    size_t tail = 0;
    turso_status_code_t rc = turso_connection_prepare_first(
        app->conn, sql, &stmt, &tail, &err);
    if (rc != TURSO_OK || stmt == NULL) {
        die("prepare", err);
    }
    rc = run_statement(stmt, 0);
    if (rc != TURSO_DONE) {
        die("execute", g_err);
    }
    for (;;) {
        rc = turso_statement_finalize(stmt, &err);
        if (rc == TURSO_IO) {
            if (turso_statement_run_io(stmt, &err) != TURSO_OK) {
                die("finalize run_io", err);
            }
            continue;
        }
        if (rc != TURSO_DONE) {
            die("finalize", err);
        }
        break;
    }
    turso_statement_deinit(stmt);
}

/* Run a query from the loop and print every row it produces. */
static void query_rows(app_t *app, const char *sql)
{
    const char *err = NULL;
    turso_statement_t *stmt = NULL;
    size_t tail = 0; /* must not be NULL: the API writes the parse offset into it */
    turso_status_code_t rc = turso_connection_prepare_first(
        app->conn, sql, &stmt, &tail, &err);
    if (rc != TURSO_OK || stmt == NULL) {
        die("prepare", err);
    }
    for (;;) {
        rc = run_statement(stmt, 1);
        if (rc == TURSO_ROW) {
            print_row(stmt);
            continue;
        }
        if (rc == TURSO_DONE) {
            break;
        }
        die("step", g_err);
    }
    for (;;) {
        rc = turso_statement_finalize(stmt, &err);
        if (rc == TURSO_IO) {
            if (turso_statement_run_io(stmt, &err) != TURSO_OK) {
                die("finalize run_io", err);
            }
            continue;
        }
        if (rc != TURSO_DONE) {
            die("finalize", err);
        }
        break;
    }
    turso_statement_deinit(stmt);
}

/* One libuv timer tick = one statement, driven from the loop. */
static void on_tick(uv_timer_t *handle)
{
    app_t *app = (app_t *)handle->data;
    char sql[256];

    app->ticks++;
    snprintf(sql, sizeof(sql), "INSERT INTO notes(message) VALUES('tick %d')", app->ticks);
    exec_sql(app, sql);

    printf("--- after tick %d ---\n", app->ticks);
    query_rows(app, "SELECT id, message FROM notes ORDER BY id");

    if (app->ticks >= 5) {
        uv_timer_stop(&app->tick);
        uv_close((uv_handle_t *)&app->tick, NULL);
    }
}

int main(int argc, char **argv)
{
    const char *path = argc > 1 ? argv[1] : "libuv-example.db";
    app_t app;
    const char *err = NULL;
    turso_config_t cfg = {NULL, "error"};
    turso_database_config_t dbcfg;
    const turso_database_t *db = NULL; /* turso_database_new wants a const ** */
    turso_connection_t *conn = NULL;   /* turso_database_connect wants a ** */

    /*
     * async_io = 1 is what makes the caller the driver of I/O: step/execute
     * return TURSO_IO instead of blocking, and we decide when to pump.
     */
    memset(&dbcfg, 0, sizeof(dbcfg));
    dbcfg.async_io = 1;
    dbcfg.path = path;
    dbcfg.vfs = "syscall";

    if (turso_setup(&cfg, &err) != TURSO_OK) {
        die("turso_setup", err);
    }
    if (turso_database_new(&dbcfg, &db, &err) != TURSO_OK) {
        die("turso_database_new", err);
    }

    /* Open and connect once, outside the loop. Note if they ask for IO. */
    if (turso_database_open(db, &err) == TURSO_IO) {
        fprintf(stderr,
                "turso_database_open requested IO, but the C ABI exposes no "
                "database-level pump (only turso_statement_run_io). Open the "
                "database before starting the event loop with a synchronous "
                "driver, or drive the open through the Rust bindings.\n");
        return 1;
    }
    if (turso_database_connect(db, &conn, &err) != TURSO_OK) {
        die("turso_database_connect", err);
    }

    app.loop = uv_default_loop();
    app.conn = (const turso_connection_t *)conn;
    app.ticks = 0;

    exec_sql(&app, "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, message TEXT)");

    /* libuv owns the pacing: a tick every 500ms, each one a full statement. */
    uv_timer_init(app.loop, &app.tick);
    app.tick.data = &app;
    uv_timer_start(&app.tick, on_tick, 0, 500);

    printf("libuv loop running; database I/O is driven from timer callbacks\n");
    uv_run(app.loop, UV_RUN_DEFAULT);

    turso_connection_close(conn, &err);
    turso_connection_deinit(conn);
    turso_database_deinit(db);
    return 0;
}
