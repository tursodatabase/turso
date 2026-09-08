/*
 * rthist.c
 * Response time histogram of the measured transactions.
 */

#include <stdio.h>

#include "rthist.h"

/* 10 µs buckets up to 10 s; anything slower lands in the last bucket. The
 * arrays are large but untouched buckets never get a page. */
#define BUCKET_MS 0.01
#define BUCKETS 1000000

extern int counting_on;
extern double cur_max_rt[];

const char *const transaction_names[5] = {"neworder", "payment", "orderstatus",
                                          "delivery", "stocklevel"};

static long hist[5][BUCKETS];

void hist_init(void) {
  int i, j;

  for (i = 0; i < 5; i++) {
    for (j = 0; j < BUCKETS; j++) {
      hist[i][j] = 0;
    }
  }
}

void hist_inc(int transaction, double rt_ms) {
  long i;

  if (rt_ms > cur_max_rt[transaction]) cur_max_rt[transaction] = rt_ms;
  if (!counting_on) return;
  i = (long)(rt_ms / BUCKET_MS);
  if (i >= BUCKETS) i = BUCKETS - 1;
  __atomic_fetch_add(&hist[transaction][i], 1, __ATOMIC_RELAXED);
}

long hist_count(int transaction) {
  long i, total = 0;

  for (i = 0; i < BUCKETS; i++) {
    total += hist[transaction][i];
  }
  return total;
}

/* Every transaction is taken to sit in the middle of its bucket. */
double hist_mean_ms(int transaction) {
  long i, total = 0;
  double sum = 0.0;

  for (i = 0; i < BUCKETS; i++) {
    total += hist[transaction][i];
    sum += hist[transaction][i] * (i + 0.5) * BUCKET_MS;
  }
  return total ? sum / total : 0.0;
}

double hist_max_ms(int transaction) {
  long i;

  for (i = BUCKETS - 1; i >= 0; i--) {
    if (hist[transaction][i]) return (i + 1) * BUCKET_MS;
  }
  return 0.0;
}

double hist_percentile_ms(int transaction, double percent) {
  long i, total, seen = 0;

  total = hist_count(transaction);
  if (total == 0) return 0.0;
  for (i = 0; i < BUCKETS; i++) {
    seen += hist[transaction][i];
    if (seen * 100.0 >= total * percent) return (i + 1) * BUCKET_MS;
  }
  return BUCKETS * BUCKET_MS;
}

void hist_write_csv(FILE *f) {
  int t;
  long i;

  fprintf(f, "engine,transaction,rt_ms,count\n");
  for (t = 0; t < 5; t++) {
    for (i = 0; i < BUCKETS; i++) {
      if (hist[t][i]) {
        fprintf(f, "%s,%s,%.2f,%ld\n", ENGINE_NAME, transaction_names[t],
                i * BUCKET_MS, hist[t][i]);
      }
    }
  }
}
