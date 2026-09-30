/*
 * driver.c
 * driver for the tpcc transactions
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/times.h>
#include <time.h>
#include "tpc.h"      /* prototypes for misc. functions */
#include "trans_if.h" /* prototypes for transacation interface calls */
#include "sequence.h"
#include "rthist.h"
#include "sb_percentile.h"
#include <sqlite3.h>

static int other_ware (int home_ware);
static int do_neword (int t_num);
static int do_payment (int t_num);
static int do_ordstat (int t_num);
static int do_delivery (int t_num);
static int do_slev (int t_num);

extern sqlite3 **ctx;
extern int num_ware;
extern int num_conn;
extern int activate_transaction;
extern int counting_on;
extern int time_start;
extern int time_end;
extern int num_trans;

extern int num_node;
extern int time_count;
extern FILE *freport_file;

extern int success[];
extern int late[];
extern int retry[];
extern int failure[];

extern int* success2[];
extern int* late2[];
extern int* retry2[];
extern int* failure2[];

extern double max_rt[];
extern double total_rt[];

extern int rt_limit[];

extern long clk_tck;
extern sb_percentile_t local_percentile;

#define MAX_RETRY 2000

int driver (int t_num)
{
    int i, j;
    instrumentation_type neword_time, payment_time, ordstat_time, delivery_time, slev_time;
    /* Actually, WaitTimes are needed... */

    //for (i = 0; i < num_trans; i++) { 

	    
    switch(seq_get()){
    case 0:
	    START_TIMING(neword_t, neword_time);
	    do_neword(t_num);
	    END_TIMING(neword_t, neword_time);
	    break;
    case 1:
	    START_TIMING(payment_t, payment_time);
	    do_payment(t_num);
	    END_TIMING(payment_t, payment_time);	      
	    break;
    case 2:
	    START_TIMING(ordstat_t, ordstat_time);
	    do_ordstat(t_num);
	    END_TIMING(ordstat_t, ordstat_time);
	    break;
    case 3:
	    START_TIMING(delivery_t, delivery_time);
	    do_delivery(t_num);
	    END_TIMING(delivery_t, delivery_time);
	    break;
    case 4:
	    START_TIMING(slev_t, slev_time);
	    do_slev(t_num);
	    END_TIMING(slev_t, slev_time);
	    break;
    default:
	    printf("Error - Unknown sequence.\n");
    }
	    
    //PRINT_TIME();
    //num_trans++;
    //}
    

    return(0);

}

/*
 * prepare data and execute the new order transaction for one order
 * officially, this is supposed to be simulated terminal I/O
 */
static int do_neword (int t_num)
{
    int c_num;
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id, ol_cnt;
    int  all_local = 1;
    int  notfound = MAXITEMS+1;  /* valid item ids are numbered consecutively
				    [1..MAXITEMS] */
    int rbk;
    int  itemid[MAX_NUM_ITEMS];
    int  supware[MAX_NUM_ITEMS];
    int  qty[MAX_NUM_ITEMS];

    if(num_node==0){
	w_id = RandomNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST);

    ol_cnt = RandomNumber(5, 15);
    rbk = RandomNumber(1, 100);

    for (i = 0; i < ol_cnt; i++) {
	itemid[i] = NURand(8191, 1, MAXITEMS);
	if ((i == ol_cnt - 1) && (rbk == 1)) {
	    itemid[i] = notfound;
	}
	if (RandomNumber(1, 100) != 1) {
	    supware[i] = w_id;
	}
	else {
	    supware[i] = other_ware(w_id);
	    all_local = 0;
	}
	qty[i] = RandomNumber(1, 10);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = neword(t_num, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
        //printf("NOT : %.3f\n", rt);

	if(rt > max_rt[0])
	  max_rt[0]=rt;
	total_rt[0] += rt;
	sb_percentile_update(&local_percentile, rt);
	hist_inc(0, rt);
	if(counting_on){
	  if( rt < rt_limit[0]){
	    success[0]++;
	    success2[0][t_num]++;
	  }else{
	    late[0]++;
	    late2[0][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[0]++;
	  retry2[0][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[0]--;
      retry2[0][t_num]--;
      failure[0]++;
      failure2[0][t_num]++;
    }

    return (0);
}

/*
 * produce the id of a valid warehouse other than home_ware
 * (assuming there is one)
 */
static int other_ware (int home_ware)
{
    int tmp;

    if (num_ware == 1) return home_ware;
    while ((tmp = RandomNumber(1, num_ware)) == home_ware);
    return tmp;
}

/*
 * prepare data and execute payment transaction
 */
static int do_payment (int t_num)
{
    int c_num;
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_w_id, c_d_id, c_id, h_amount;
    char c_last[17];

    if(num_node==0){
	w_id = RandomNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    h_amount = RandomNumber(1,5000);
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }
    if (RandomNumber(1, 100) <= 85) {
        c_w_id = w_id;
        c_d_id = d_id;
    }else{
        c_w_id = other_ware(w_id);
        c_d_id = RandomNumber(1, DIST_PER_WARE);
    }

    clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = payment(t_num, w_id, d_id, byname, c_w_id, c_d_id, c_id, c_last, h_amount);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[1])
	  max_rt[1]=rt;
	total_rt[1] += rt;
	hist_inc(1, rt);
	if(counting_on){
	  if( rt < rt_limit[1]){
	    success[1]++;
	    success2[1][t_num]++;
	  }else{
	    late[1]++;
	    late2[1][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[1]++;
	  retry2[1][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[1]--;
      retry2[1][t_num]--;
      failure[1]++;
      failure2[1][t_num]++;
    }

    return (0);
}

/*
 * prepare data and execute order status transaction
 */
static int do_ordstat (int t_num)
{
    int c_num;
    int byname,i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, c_id;
    char c_last[16];

    if(num_node==0){
	w_id = RandomNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE);
    c_id = NURand(1023, 1, CUST_PER_DIST); 
    Lastname(NURand(255,0,999), c_last); 
    if (RandomNumber(1, 100) <= 60) {
        byname = 1; /* select by last name */
    }else{
        byname = 0; /* select by customer id */
    }

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = ordstat(t_num, w_id, d_id, byname, c_id, c_last);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[2])
	  max_rt[2]=rt;
	total_rt[2] += rt;
	hist_inc(2, rt);
	if(counting_on){
	  if( rt < rt_limit[2]){
	    success[2]++;
	    success2[2][t_num]++;
	  }else{
	    late[2]++;
	    late2[2][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[2]++;
	  retry2[2][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[2]--;
      retry2[2][t_num]--;
      failure[2]++;
      failure2[2][t_num]++;
    }

    return (0);

}

/*
 * execute delivery transaction
 */
static int do_delivery (int t_num)
{
    int c_num;
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, o_carrier_id;

    if(num_node==0){
	w_id = RandomNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    o_carrier_id = RandomNumber(1, 10);

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = delivery(t_num, w_id, o_carrier_id);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[3])
	  max_rt[3]=rt;
	total_rt[3] += rt;
	hist_inc(3, rt );
	if(counting_on){
	  if( rt < rt_limit[3]){
	    success[3]++;
	    success2[3][t_num]++;
	  }else{
	    late[3]++;
	    late2[3][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[3]++;
	  retry2[3][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[3]--;
      retry2[3][t_num]--;
      failure[3]++;
      failure2[3][t_num]++;
    }

    return (0);

}

/* 
 * prepare data and execute the stock level transaction
 */
static int do_slev (int t_num)
{
    int c_num;
    int i,ret;
    clock_t clk1,clk2;
    double rt;
    struct timespec tbuf1;
    struct timespec tbuf2;
    int  w_id, d_id, level;

    if(num_node==0){
	w_id = RandomNumber(1, num_ware);
    }else{
	c_num = ((num_node * t_num)/num_conn); /* drop moduls */
	w_id = RandomNumber(1 + (num_ware * c_num)/num_node,
			    (num_ware * (c_num + 1))/num_node);
    }
    d_id = RandomNumber(1, DIST_PER_WARE); 
    level = RandomNumber(10, 20); 

      clk1 = clock_gettime(CLOCK_MONOTONIC, &tbuf1 );
    for (i = 0; i < MAX_RETRY; i++) {
      ret = slev(t_num, w_id, d_id, level);
      clk2 = clock_gettime(CLOCK_MONOTONIC, &tbuf2 );

      if(ret){

	rt = (double)(tbuf2.tv_sec * 1000.0 + tbuf2.tv_nsec/1000000.0-tbuf1.tv_sec * 1000.0 - tbuf1.tv_nsec/1000000.0);
	if(rt > max_rt[4])
	  max_rt[4]=rt;
	total_rt[4] += rt;
	hist_inc(4, rt );
	if(counting_on){
	  if( rt < rt_limit[4]){
	    success[4]++;
	    success2[4][t_num]++;
	  }else{
	    late[4]++;
	    late2[4][t_num]++;
	  }
	}

	return (1); /* end */
      }else{

	if(counting_on){
	  retry[4]++;
	  retry2[4][t_num]++;
	}

      }
    }

    if(counting_on){
      retry[4]--;
      retry2[4][t_num]--;
      failure[4]++;
      failure2[4][t_num]++;
    }

    return (0);

}
