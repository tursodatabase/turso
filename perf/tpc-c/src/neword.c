/*
 * -*-C-*- 
 * neword.pc 
 * corresponds to A.1 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

#define pick_dist_info(ol_dist_info,ol_supply_w_id) \
switch(ol_supply_w_id) { \
case 1: strncpy(ol_dist_info, s_dist_01, 25); break; \
case 2: strncpy(ol_dist_info, s_dist_02, 25); break; \
case 3: strncpy(ol_dist_info, s_dist_03, 25); break; \
case 4: strncpy(ol_dist_info, s_dist_04, 25); break; \
case 5: strncpy(ol_dist_info, s_dist_05, 25); break; \
case 6: strncpy(ol_dist_info, s_dist_06, 25); break; \
case 7: strncpy(ol_dist_info, s_dist_07, 25); break; \
case 8: strncpy(ol_dist_info, s_dist_08, 25); break; \
case 9: strncpy(ol_dist_info, s_dist_09, 25); break; \
case 10: strncpy(ol_dist_info, s_dist_10, 25); break; \
}

extern sqlite3 **ctx;
extern sqlite3_stmt ***stmt;

#define NNULL ((void *)0)

/*
 * the new order transaction
 */
int neword( int t_num,
	    int w_id_arg,		/* warehouse id */
	    int d_id_arg,		/* district id */
	    int c_id_arg,		/* customer id */
	    int o_ol_cnt_arg,	        /* number of items */
	    int o_all_local_arg,	/* are all order lines local */
	    int itemid[],		/* ids of items to be ordered */
	    int supware[],		/* warehouses supplying items */
	    int qty[]		        /* quantity of each item */
)
{

	int ret;
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            o_ol_cnt = o_ol_cnt_arg;
	int            o_all_local = o_all_local_arg;
	float           c_discount;
	char            c_last[17];
	char            c_credit[3];
	float           w_tax;
	int            d_next_o_id;
	float           d_tax;
	char            datetime[81];
	int            o_id;
	char            i_name[25];
	float           i_price;
	char            i_data[51];
	int            ol_i_id;
	int            s_quantity;
	char            s_data[51];
	char            s_dist_01[25];
	char            s_dist_02[25];
	char            s_dist_03[25];
	char            s_dist_04[25];
	char            s_dist_05[25];
	char            s_dist_06[25];
	char            s_dist_07[25];
	char            s_dist_08[25];
	char            s_dist_09[25];
	char            s_dist_10[25];
	char            ol_dist_info[25];
	int            ol_supply_w_id;
	float           ol_amount;
	int            ol_number;
	int            ol_quantity;

	char            iname[MAX_NUM_ITEMS][MAX_ITEM_LEN];
	char            bg[MAX_NUM_ITEMS];
	float           amt[MAX_NUM_ITEMS];
	float           price[MAX_NUM_ITEMS];
	int            stock[MAX_NUM_ITEMS];
	float           total = 0.0;

	int            min_num;
	int            i,j,tmp,swp;
	int            ol_num_seq[MAX_NUM_ITEMS];

	int             proceed = 0;
 	struct timespec tbuf1,tbuf_start;
	clock_t clk1,clk_start;	


	sqlite3_stmt*   sqlite_stmt;
	int num_cols;
	
	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/*EXEC SQL CONTEXT USE :ctx[t_num];*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);
	clk_start = clock_gettime(CLOCK_REALTIME, &tbuf_start );

	proceed = 1;
	/*EXEC_SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
	        FROM customer, warehouse
	        WHERE w_id = :w_id 
		AND c_w_id = w_id 
		AND c_d_id = :d_id 
		AND c_id = :c_id;*/
	sqlite_stmt = stmt[t_num][0];

	sqlite3_bind_int64(sqlite_stmt, 1, w_id);
	sqlite3_bind_int64(sqlite_stmt, 2, d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, c_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 4) goto sqlerr;
	
		c_discount = sqlite3_column_double(sqlite_stmt, 0);
		strcpy(c_last, sqlite3_column_text(sqlite_stmt, 1));
		strcpy(c_credit, sqlite3_column_text(sqlite_stmt, 2));
		w_tax = sqlite3_column_double(sqlite_stmt, 3);
	}

	sqlite3_reset(sqlite_stmt);

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 2;
	/*EXEC_SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
	        FROM district
	        WHERE d_id = :d_id
		AND d_w_id = :w_id
		FOR UPDATE;*/

	sqlite_stmt = stmt[t_num][1];

	sqlite3_bind_int64(sqlite_stmt, 1, d_id);
	sqlite3_bind_int64(sqlite_stmt, 2, w_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 2) goto sqlerr;

		d_next_o_id = sqlite3_column_int64(sqlite_stmt, 0);
		d_tax = sqlite3_column_double(sqlite_stmt, 1);
	}

	sqlite3_reset(sqlite_stmt);

	proceed = 3;
	/*EXEC_SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
	        WHERE d_id = :d_id 
		AND d_w_id = :w_id;*/

	sqlite_stmt = stmt[t_num][2];

	sqlite3_bind_int64(sqlite_stmt, 1, d_next_o_id);
	sqlite3_bind_int64(sqlite_stmt, 2, d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, w_id);

	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);

	o_id = d_next_o_id;

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif

	proceed = 4;
	/*EXEC_SQL INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
			             o_entry_d, o_ol_cnt, o_all_local)
		VALUES(:o_id, :d_id, :w_id, :c_id, 
		       :datetime,
                       :o_ol_cnt, :o_all_local);*/


	sqlite_stmt = stmt[t_num][3];

	sqlite3_bind_int64(sqlite_stmt, 1, o_id);
	sqlite3_bind_int64(sqlite_stmt, 2, d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, w_id);
	sqlite3_bind_int64(sqlite_stmt, 4, c_id);
	sqlite3_bind_text(sqlite_stmt, 5, datetime, -1, SQLITE_STATIC);
	sqlite3_bind_int64(sqlite_stmt, 6, o_ol_cnt);
	sqlite3_bind_int64(sqlite_stmt, 7, o_all_local);
			
	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);

#ifdef DEBUG
	printf("n %d\n",proceed);
#endif
	proceed = 5;
	/* EXEC_SQL INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
	   VALUES (:o_id,:d_id,:w_id); */

	sqlite_stmt = stmt[t_num][4];

	sqlite3_bind_int64(sqlite_stmt, 1, o_id);
	sqlite3_bind_int64(sqlite_stmt, 2, d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, w_id);
			
	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);

	/* sort orders to avoid DeadLock */
	for (i = 0; i < o_ol_cnt; i++) {
		ol_num_seq[i]=i;
	}
	for (i = 0; i < (o_ol_cnt - 1); i++) {
		tmp = (MAXITEMS + 1) * supware[ol_num_seq[i]] + itemid[ol_num_seq[i]];
		min_num = i;
		for ( j = i+1; j < o_ol_cnt; j++) {
		  if ( (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]] < tmp ){
		    tmp = (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]];
		    min_num = j;
		  }
		}
		if ( min_num != i ){
		  swp = ol_num_seq[min_num];
		  ol_num_seq[min_num] = ol_num_seq[i];
		  ol_num_seq[i] = swp;
		}
	}


	for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
		ol_supply_w_id = supware[ol_num_seq[ol_number - 1]];
		ol_i_id = itemid[ol_num_seq[ol_number - 1]];
		ol_quantity = qty[ol_num_seq[ol_number - 1]];

		/* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
		proceed = 6;
		/*EXEC_SQL SELECT i_price, i_name, i_data
			INTO :i_price, :i_name, :i_data
		        FROM item
		        WHERE i_id = :ol_i_id;*/

		sqlite_stmt = stmt[t_num][5];

		sqlite3_bind_int64(sqlite_stmt, 1, ol_i_id);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 3) goto sqlerr;

			i_price = sqlite3_column_double(sqlite_stmt, 0);
			strcpy(i_name, sqlite3_column_text(sqlite_stmt, 1));
			strcpy(i_data, sqlite3_column_text(sqlite_stmt, 2));
		}				

		sqlite3_reset(sqlite_stmt);

		price[ol_num_seq[ol_number - 1]] = i_price;
		strncpy(iname[ol_num_seq[ol_number - 1]], i_name, 25);

		/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif
		proceed = 7;

		/*EXEC_SQL SELECT s_quantity, s_data, s_dist_01, s_dist_02,
		                s_dist_03, s_dist_04, s_dist_05, s_dist_06,
		                s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
		             :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
		             :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
		        FROM stock
		        WHERE s_i_id = :ol_i_id 
			AND s_w_id = :ol_supply_w_id
			FOR UPDATE;*/

		sqlite_stmt = stmt[t_num][6];

		sqlite3_bind_int64(sqlite_stmt, 1, ol_i_id);
		sqlite3_bind_int64(sqlite_stmt, 2, ol_supply_w_id);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 12) goto sqlerr;

			s_quantity = sqlite3_column_int64(sqlite_stmt, 0);
			strcpy(s_data, sqlite3_column_text(sqlite_stmt, 1));
			strcpy(s_dist_01, sqlite3_column_text(sqlite_stmt, 2));
			strcpy(s_dist_02, sqlite3_column_text(sqlite_stmt, 3));
			strcpy(s_dist_03, sqlite3_column_text(sqlite_stmt, 4));
			strcpy(s_dist_04, sqlite3_column_text(sqlite_stmt, 5));
			strcpy(s_dist_05, sqlite3_column_text(sqlite_stmt, 6));
			strcpy(s_dist_06, sqlite3_column_text(sqlite_stmt, 7));
			strcpy(s_dist_07, sqlite3_column_text(sqlite_stmt, 8));
			strcpy(s_dist_08, sqlite3_column_text(sqlite_stmt, 9));
			strcpy(s_dist_09, sqlite3_column_text(sqlite_stmt, 10));
			strcpy(s_dist_10, sqlite3_column_text(sqlite_stmt, 11));
		}

		sqlite3_reset(sqlite_stmt);

		pick_dist_info(ol_dist_info, d_id);	/* pick correct
							 * s_dist_xx */

		stock[ol_num_seq[ol_number - 1]] = s_quantity;

		if ((strstr(i_data, "original") != NULL) &&
		    (strstr(s_data, "original") != NULL))
			bg[ol_num_seq[ol_number - 1]] = 'B';
		else
			bg[ol_num_seq[ol_number - 1]] = 'G';

		if (s_quantity > ol_quantity)
			s_quantity = s_quantity - ol_quantity;
		else
			s_quantity = s_quantity - ol_quantity + 91;

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		proceed = 8;
		/*EXEC_SQL UPDATE stock SET s_quantity = :s_quantity
		        WHERE s_i_id = :ol_i_id 
			AND s_w_id = :ol_supply_w_id;*/

		sqlite_stmt = stmt[t_num][7];

		sqlite3_bind_int64(sqlite_stmt, 1, s_quantity);
		sqlite3_bind_int64(sqlite_stmt, 2, ol_i_id);
		sqlite3_bind_int64(sqlite_stmt, 3, ol_supply_w_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

		ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
		amt[ol_num_seq[ol_number - 1]] = ol_amount;
		total += ol_amount;

#ifdef DEBUG
		printf("n %d\n",proceed);
#endif

		proceed = 9;
		/*EXEC_SQL INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
						 ol_number, ol_i_id, 
						 ol_supply_w_id, ol_quantity, 
						 ol_amount, ol_dist_info)
			VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
				:ol_supply_w_id, :ol_quantity, :ol_amount,
				:ol_dist_info);*/

		sqlite_stmt = stmt[t_num][8];

		sqlite3_bind_int64(sqlite_stmt, 1, o_id);
		sqlite3_bind_int64(sqlite_stmt, 2, d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, w_id);
		sqlite3_bind_int64(sqlite_stmt, 4, ol_number);
		sqlite3_bind_int64(sqlite_stmt, 5, ol_i_id);
		sqlite3_bind_int64(sqlite_stmt, 6, ol_supply_w_id);
		sqlite3_bind_int64(sqlite_stmt, 7, ol_amount);
		sqlite3_bind_double(sqlite_stmt, 8, ol_supply_w_id);
		sqlite3_bind_text(sqlite_stmt, 9, ol_dist_info, -1, SQLITE_STATIC);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

	}			/* End Order Lines */

#ifdef DEBUG
	printf("insert 3\n");
	fflush(stdout);
#endif

	/*EXEC_SQL COMMIT WORK;*/
	//if( sqlite3_exec(ctx[t_num], "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	clk1 = clock_gettime(CLOCK_REALTIME, &tbuf1 );

	return (1);

invaliditem:
	/*EXEC_SQL ROLLBACK WORK;*/
	if( sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	/* printf("Item number is not valid\n"); */
	return (1); /* OK? */

sqlerr:
	fprintf(stderr,"neword %d:%d\n",t_num,proceed);
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));
      	//error(ctx[t_num],mysql_stmt);
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
sqlerrerr:
	return (0);
}

