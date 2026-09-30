/*
 * -*-C-*-
 * delivery.pc
 * corresponds to A.4 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

extern sqlite3 **ctx;
extern sqlite3_stmt ***stmt;

#define NNULL ((void *)0)

int delivery( int t_num,
	      int w_id_arg,
	      int o_carrier_id_arg
)
{
	int ret;
	int            w_id = w_id_arg;
	int            o_carrier_id = o_carrier_id_arg;
	int            d_id;
	int            c_id;
	int            no_o_id;
	float           ol_total;
	char            datetime[81];

	int proceed = 0;

	sqlite3_stmt *sqlite_stmt;
	int num_cols;
	
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* For each district in warehouse */
	/* printf("W: %d\n", w_id); */

	for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
	        proceed = 1;
		/*EXEC_SQL SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
		                FROM new_orders
		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/

		sqlite_stmt = stmt[t_num][25];

		sqlite3_bind_int64(sqlite_stmt, 1, d_id);
		sqlite3_bind_int64(sqlite_stmt, 2, w_id);

		ret = sqlite3_step(sqlite_stmt);		
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 1) goto sqlerr;

			no_o_id = sqlite3_column_int64(sqlite_stmt, 0);
		}

		sqlite3_reset(sqlite_stmt);
		
		if(no_o_id == 0) continue;
		proceed = 2;
		/*EXEC_SQL DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
		  AND no_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][26];

		sqlite3_bind_int64(sqlite_stmt, 1, no_o_id);
		sqlite3_bind_int64(sqlite_stmt, 2, d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, w_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

		proceed = 3;
		/*EXEC_SQL SELECT o_c_id INTO :c_id FROM orders
		                WHERE o_id = :no_o_id AND o_d_id = :d_id
				AND o_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][27];

		sqlite3_bind_int64(sqlite_stmt, 1, no_o_id);
		sqlite3_bind_int64(sqlite_stmt, 2, d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, w_id);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 1) goto sqlerr;

			c_id = sqlite3_column_int64(sqlite_stmt, 0);
		}

		sqlite3_reset(sqlite_stmt);

		proceed = 4;
		/*EXEC_SQL UPDATE orders SET o_carrier_id = :o_carrier_id
		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
				o_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][28];

		sqlite3_bind_int64(sqlite_stmt, 1, o_carrier_id);
		sqlite3_bind_int64(sqlite_stmt, 2, no_o_id);
		sqlite3_bind_int64(sqlite_stmt, 3, d_id);
		sqlite3_bind_int64(sqlite_stmt, 4, w_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
		
		proceed = 5;
		/*EXEC_SQL UPDATE order_line
		                SET ol_delivery_d = :datetime
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
				ol_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][29];

		sqlite3_bind_text(sqlite_stmt, 1, datetime, -1, SQLITE_STATIC);
		sqlite3_bind_int64(sqlite_stmt, 2, no_o_id);
		sqlite3_bind_int64(sqlite_stmt, 3, d_id);
		sqlite3_bind_int64(sqlite_stmt, 4, w_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

		proceed = 6;
		/*EXEC_SQL SELECT SUM(ol_amount) INTO :ol_total
		                FROM order_line
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
				AND ol_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][30];

		sqlite3_bind_int64(sqlite_stmt, 1, no_o_id);
		sqlite3_bind_int64(sqlite_stmt, 2, d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, w_id);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 1) goto sqlerr;

			ol_total = sqlite3_column_double(sqlite_stmt, 0);
		}

		sqlite3_reset(sqlite_stmt);

		proceed = 7;
		/*EXEC_SQL UPDATE customer SET c_balance = c_balance + :ol_total ,
		                             c_delivery_cnt = c_delivery_cnt + 1
		                WHERE c_id = :c_id AND c_d_id = :d_id AND
				c_w_id = :w_id;*/
		sqlite_stmt = stmt[t_num][31];

		sqlite3_bind_double(sqlite_stmt, 1, ol_total);
		sqlite3_bind_int64(sqlite_stmt, 2, c_id);
		sqlite3_bind_int64(sqlite_stmt, 3, d_id);
		sqlite3_bind_int64(sqlite_stmt, 4, w_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

		/*EXEC_SQL COMMIT WORK;*/
		//if( sqlite3_exec(ctx[t_num], "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

		/* printf("D: %d, O: %d, time: %d\n", d_id, o_id, tad); */

	}
	/*EXEC_SQL COMMIT WORK;*/
	return (1);

sqlerr:
        fprintf(stderr, "delivery %d:%d\n",t_num,proceed);
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));

	//error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
sqlerrerr:
	return (0);
}
