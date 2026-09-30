/*
 * -*-C-*- 
 * ordstat.pc 
 * corresponds to A.3 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

extern sqlite3 **ctx;
extern sqlite3_stmt ***stmt;

/*
 * the order status transaction
 */
int ordstat( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_id_arg,		/* customer id */
	     char c_last_arg[]	        /* customer last name, format? */
)
{
	int ret;
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	int            c_d_id = d_id;
	int            c_w_id = w_id;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	float           c_balance;
	int            o_id;
	char            o_entry_d[25];
	int            o_carrier_id;
	int            ol_i_id;
	int            ol_supply_w_id;
	int            ol_quantity;
	float           ol_amount;
	char            ol_delivery_d[25];
	int            namecnt;

	int             n;
	int             proceed = 0;

	sqlite3_stmt *sqlite_stmt;
	int num_cols;
	int bytes;
	
	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	if (byname) {
		strcpy(c_last, c_last_arg);
		proceed = 1;
		/*EXEC_SQL SELECT count(c_id)
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/

		sqlite_stmt = stmt[t_num][20];

		sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_text(sqlite_stmt, 3, c_last, -1, SQLITE_STATIC);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 1) goto sqlerr;

			namecnt = sqlite3_column_int64(sqlite_stmt, 0);
		}

		sqlite3_reset(sqlite_stmt);

		proceed = 2;
		/*EXEC_SQL DECLARE c_byname_o CURSOR FOR
		        SELECT c_balance, c_first, c_middle, c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_last = :c_last
			ORDER BY c_first;
		proceed = 3;
		EXEC_SQL OPEN c_byname_o;*/

		sqlite_stmt = stmt[t_num][21];

		sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_text(sqlite_stmt, 3, c_last, -1, SQLITE_STATIC);

		if (namecnt % 2)
			namecnt++;	/* Locate midpoint customer; */

		for (n = 0; n < namecnt / 2; n++) {
			ret = sqlite3_step(sqlite_stmt);
			if (ret != SQLITE_DONE) {
				if (ret != SQLITE_ROW) goto sqlerr;
				num_cols = sqlite3_column_count(sqlite_stmt);
				if (num_cols != 4) goto sqlerr;
				c_balance = sqlite3_column_double(sqlite_stmt, 0);
				strcpy(c_first, sqlite3_column_text(sqlite_stmt, 1));
				strcpy(c_middle, sqlite3_column_text(sqlite_stmt, 2));
				strcpy(c_last, sqlite3_column_text(sqlite_stmt, 3));
			}
		}

		sqlite3_reset(sqlite_stmt);

		proceed = 5;
		/*EXEC_SQL CLOSE  c_byname_o;*/

	} else {		/* by number */
		proceed = 6;
		/*EXEC_SQL SELECT c_balance, c_first, c_middle, c_last
			INTO :c_balance, :c_first, :c_middle, :c_last
		        FROM customer
		        WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
			AND c_id = :c_id;*/

		sqlite_stmt = stmt[t_num][22];

		sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_text(sqlite_stmt, 3, c_last, -1, SQLITE_STATIC);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 4) goto sqlerr;

			c_balance = sqlite3_column_double(sqlite_stmt, 0);
			strcpy(c_first, sqlite3_column_text(sqlite_stmt, 1));
			strcpy(c_middle, sqlite3_column_text(sqlite_stmt, 2));
			strcpy(c_last, sqlite3_column_text(sqlite_stmt, 3));
		}

		sqlite3_reset(sqlite_stmt);
	}

	/* find the most recent order for this customer */

	proceed = 7;
	/*EXEC_SQL SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0)
		INTO :o_id, :o_entry_d, :o_carrier_id
	        FROM orders
	        WHERE o_w_id = :c_w_id
		AND o_d_id = :c_d_id
		AND o_c_id = :c_id
		AND o_id = (SELECT MAX(o_id)
		    	    FROM orders
		    	    WHERE o_w_id = :c_w_id
		  	    AND o_d_id = :c_d_id
		    	    AND o_c_id = :c_id);*/

	sqlite_stmt = stmt[t_num][23];

	sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
	sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, c_id);
	sqlite3_bind_int64(sqlite_stmt, 4, c_w_id);
	sqlite3_bind_int64(sqlite_stmt, 5, c_d_id);
	sqlite3_bind_int64(sqlite_stmt, 6, c_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 3) goto sqlerr;

		o_id = sqlite3_column_int64(sqlite_stmt, 0);
		strcpy(o_entry_d, sqlite3_column_text(sqlite_stmt, 1));
		o_carrier_id = sqlite3_column_int64(sqlite_stmt, 2);
	}
	/* find all the items in this order */

	sqlite3_reset(sqlite_stmt);

	proceed = 8;
	/*EXEC_SQL DECLARE c_items CURSOR FOR
		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
                       ol_delivery_d
		FROM order_line
	        WHERE ol_w_id = :c_w_id
		AND ol_d_id = :c_d_id
		AND ol_o_id = :o_id;*/

	sqlite_stmt = stmt[t_num][24];

	sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
	sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, o_id);

	for(;;) {
		ret = sqlite3_step(sqlite_stmt);

		if (ret == SQLITE_DONE)
			break;
		
		if (ret == SQLITE_ROW) {
			proceed = 10;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 5) goto sqlerr;
			
			ol_i_id = sqlite3_column_int64(sqlite_stmt, 0);
			ol_supply_w_id = sqlite3_column_int64(sqlite_stmt, 1);
			ol_quantity = sqlite3_column_int64(sqlite_stmt, 2);
			ol_amount = sqlite3_column_double(sqlite_stmt, 3);
			bytes = sqlite3_column_bytes(sqlite_stmt, 4);
			if (bytes)
				strcpy(ol_delivery_d, sqlite3_column_text(sqlite_stmt, 4));
		}
		else
			goto sqlerr;
	}

	sqlite3_reset(sqlite_stmt);

	/*proceed = 9;
	EXEC_SQL OPEN c_items;

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/
	
done:
	/*EXEC_SQL CLOSE c_items;*/
        /*EXEC_SQL COMMIT WORK;*/
	//if( sqlite3_exec(ctx[t_num], "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr, "ordstat %d:%d\n",t_num,proceed);
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));

	//error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
sqlerrerr:
	return (0);
}

