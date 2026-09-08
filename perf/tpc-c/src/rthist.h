/*
 * rthist.h
 * Response time histogram of the measured transactions.
 */

#include <stdio.h>

/* Short names of the five transaction types, indexed like every counter. */
extern const char *const transaction_names[5];

void hist_init(void);
/* Records one finished transaction; only counted while measuring. */
void hist_inc(int transaction, double rt_ms);
long hist_count(int transaction);
double hist_mean_ms(int transaction);
double hist_max_ms(int transaction);
double hist_percentile_ms(int transaction, double percent);
/* One row per non-empty bucket: engine, transaction, bucket start in ms,
 * count. */
void hist_write_csv(FILE *f);
