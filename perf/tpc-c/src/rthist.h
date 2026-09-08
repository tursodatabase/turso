/*
 * rthist.h
 * Response time histogram of the measured transactions.
 */

void hist_init(void);
/* Records one finished transaction; only counted while measuring. */
void hist_inc(int transaction, double rt_ms);
long hist_count(int transaction);
double hist_mean_ms(int transaction);
double hist_max_ms(int transaction);
double hist_percentile_ms(int transaction, double percent);
