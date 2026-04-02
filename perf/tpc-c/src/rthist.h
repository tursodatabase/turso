/*
 * rthist.h
 */

void hist_init();
void hist_inc( int transaction, double rtclk );
double hist_ckp( int transaction );
void hist_report();
