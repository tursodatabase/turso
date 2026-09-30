/*
 * -*-C-*-  
 * payment.pc 
 * corresponds to A.2 in appendix A
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

extern sqlite3 **ctx;
extern sqlite3_stmt ***stmt;

#define NNULL ((void *)0)

/*
 * the payment transaction
 */
int payment( int t_num,
	     int w_id_arg,		/* warehouse id */
	     int d_id_arg,		/* district id */
	     int byname,		/* select by c_id or c_last? */
	     int c_w_id_arg,
	     int c_d_id_arg,
	     int c_id_arg,		/* customer id */
	     char c_last_arg[],	        /* customer last name */
	     float h_amount_arg	        /* payment amount */
)
{
	int ret;
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            c_id = c_id_arg;
	char            w_name[11];
	char            w_street_1[21];
	char            w_street_2[21];
	char            w_city[21];
	char            w_state[3];
	char            w_zip[10];
	int            c_d_id = c_d_id_arg;
	int            c_w_id = c_w_id_arg;
	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	char            c_street_1[21];
	char            c_street_2[21];
	char            c_city[21];
	char            c_state[3];
	char            c_zip[10];
	char            c_phone[17];
	char            c_since[20];
	char            c_credit[4];
	int            c_credit_lim;
	float           c_discount;
	float           c_balance;
	char            c_data[502];
	char            c_new_data[502];
	float           h_amount = h_amount_arg;
	char            h_data[26];
	char            d_name[11];
	char            d_street_1[21];
	char            d_street_2[21];
	char            d_city[21];
	char            d_state[3];
	char            d_zip[10];
	int            namecnt;
	char            datetime[81];

	int             n;
	int             proceed = 0;
	int bytes;
	
	sqlite3_stmt *sqlite_stmt;
	int num_cols;
	
	/* EXEC SQL WHENEVER NOT FOUND GOTO sqlerr; */
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	proceed = 1;
	/*EXEC_SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
	  WHERE w_id =:w_id;*/

	sqlite_stmt = stmt[t_num][9];
		
	sqlite3_bind_double(sqlite_stmt, 1, h_amount);
	sqlite3_bind_int64(sqlite_stmt, 2, w_id);

	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);
	
	proceed = 2;
	/*EXEC_SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
	                w_name
	                INTO :w_street_1, :w_street_2, :w_city, :w_state,
				:w_zip, :w_name
	                FROM warehouse
	                WHERE w_id = :w_id;*/

	sqlite_stmt = stmt[t_num][10];

	sqlite3_bind_int64(sqlite_stmt, 1, w_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 6) goto sqlerr;

		strcpy(w_street_1, sqlite3_column_text(sqlite_stmt, 0));
		strcpy(w_street_2, sqlite3_column_text(sqlite_stmt, 1));
		strcpy(w_city, sqlite3_column_text(sqlite_stmt, 2));
		strcpy(w_state, sqlite3_column_text(sqlite_stmt, 3));
		strcpy(w_zip, sqlite3_column_text(sqlite_stmt, 4));
		strcpy(w_name, sqlite3_column_text(sqlite_stmt, 5));
	}
	
	sqlite3_reset(sqlite_stmt);
	proceed = 3;
	/*EXEC_SQL UPDATE district SET d_ytd = d_ytd + :h_amount
			WHERE d_w_id = :w_id 
			AND d_id = :d_id;*/

	sqlite_stmt = stmt[t_num][11];

	sqlite3_bind_double(sqlite_stmt, 1, h_amount);
	sqlite3_bind_int64(sqlite_stmt, 2, w_id);
	sqlite3_bind_int64(sqlite_stmt, 3, d_id);

	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);
	proceed = 4;
	/*EXEC_SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip,
	                d_name
	                INTO :d_street_1, :d_street_2, :d_city, :d_state,
				:d_zip, :d_name
	                FROM district
	                WHERE d_w_id = :w_id 
			AND d_id = :d_id;*/


	sqlite_stmt = stmt[t_num][12];

	sqlite3_bind_int64(sqlite_stmt, 1, w_id);
	sqlite3_bind_int64(sqlite_stmt, 2, d_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 6) goto sqlerr;

		strcpy(d_street_1, sqlite3_column_text(sqlite_stmt, 0));
		strcpy(d_street_2, sqlite3_column_text(sqlite_stmt, 1));
		strcpy(d_city, sqlite3_column_text(sqlite_stmt, 2));
		strcpy(d_state, sqlite3_column_text(sqlite_stmt, 3));
		strcpy(d_zip, sqlite3_column_text(sqlite_stmt, 4));
		strcpy(d_name, sqlite3_column_text(sqlite_stmt, 5));
	}

	sqlite3_reset(sqlite_stmt);

	if (byname) {
		strcpy(c_last, c_last_arg);

		proceed = 5;
		/*EXEC_SQL SELECT count(c_id) 
			INTO :namecnt
		        FROM customer
			WHERE c_w_id = :c_w_id
			AND c_d_id = :c_d_id
		        AND c_last = :c_last;*/


		sqlite_stmt = stmt[t_num][13];

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

		/*EXEC_SQL DECLARE c_byname_p CURSOR FOR
		        SELECT c_id
		        FROM customer
		        WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_last = :c_last
			ORDER BY c_first;

			EXEC_SQL OPEN c_byname_p;*/

		sqlite_stmt = stmt[t_num][14];

		sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_text(sqlite_stmt, 3, c_last, -1, SQLITE_STATIC);

		if (namecnt % 2)
			namecnt++;

		for (n = 0; n < namecnt / 2; n++) {
			ret = sqlite3_step(sqlite_stmt);
			if (ret != SQLITE_DONE) {
				if (ret != SQLITE_ROW) goto sqlerr;
				num_cols = sqlite3_column_count(sqlite_stmt);
				if (num_cols != 1) goto sqlerr;
		
				c_id = sqlite3_column_int64(sqlite_stmt, 0);
			}
		}

		sqlite3_reset(sqlite_stmt);

	}

	proceed = 6;
	/*EXEC_SQL SELECT c_first, c_middle, c_last, c_street_1,
		        c_street_2, c_city, c_state, c_zip, c_phone,
		        c_credit, c_credit_lim, c_discount, c_balance,
		        c_since
		INTO :c_first, :c_middle, :c_last, :c_street_1,
		     :c_street_2, :c_city, :c_state, :c_zip, :c_phone,
		     :c_credit, :c_credit_lim, :c_discount, :c_balance,
		     :c_since
		FROM customer
	        WHERE c_w_id = :c_w_id 
	        AND c_d_id = :c_d_id 
		AND c_id = :c_id
		FOR UPDATE;*/

	sqlite_stmt = stmt[t_num][15];

	sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
	sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
	sqlite3_bind_int64(sqlite_stmt, 3, c_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 14) goto sqlerr;

		strcpy(c_first, sqlite3_column_text(sqlite_stmt, 0));
		strcpy(c_middle, sqlite3_column_text(sqlite_stmt, 1));
		strcpy(c_last, sqlite3_column_text(sqlite_stmt, 2));
		strcpy(c_street_1, sqlite3_column_text(sqlite_stmt, 3));
		strcpy(c_street_2, sqlite3_column_text(sqlite_stmt, 4));
		strcpy(c_city, sqlite3_column_text(sqlite_stmt, 5));
		strcpy(c_state, sqlite3_column_text(sqlite_stmt, 6));
		strcpy(c_zip, sqlite3_column_text(sqlite_stmt, 7));
		strcpy(c_phone, sqlite3_column_text(sqlite_stmt, 8));
		strcpy(c_credit, sqlite3_column_text(sqlite_stmt, 9));
		c_credit_lim = sqlite3_column_int64(sqlite_stmt, 10);
		c_discount = sqlite3_column_double(sqlite_stmt, 11);
		c_balance = sqlite3_column_double(sqlite_stmt, 12);
		strcpy(c_since, sqlite3_column_text(sqlite_stmt, 13));
	}

	sqlite3_reset(sqlite_stmt);

	c_balance = c_balance - h_amount;
	c_credit[2] = '\0';
	if (strstr(c_credit, "BC")) {
		proceed = 7;
		/*EXEC_SQL SELECT c_data 
			INTO :c_data
		        FROM customer
		        WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id; */

		sqlite_stmt = stmt[t_num][16];

		sqlite3_bind_int64(sqlite_stmt, 1, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, c_id);

		ret = sqlite3_step(sqlite_stmt);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt);
			if (num_cols != 1) goto sqlerr;

			strcpy(c_data, sqlite3_column_text(sqlite_stmt, 0));
		}

		sqlite3_reset(sqlite_stmt);

		sprintf(c_new_data, 
			"| %4d %2d %4d %2d %4d $%7.2f %12c %24c",
			c_id, c_d_id, c_w_id, d_id,
			w_id, h_amount,
			datetime, c_data);

		strncat(c_new_data, c_data, 
			500 - strlen(c_new_data));

		c_new_data[500] = '\0';

		proceed = 8;
		/*EXEC_SQL UPDATE customer
			SET c_balance = :c_balance, c_data = :c_new_data
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/

		sqlite_stmt = stmt[t_num][17];

		sqlite3_bind_double(sqlite_stmt, 1, c_balance);
		sqlite3_bind_text(sqlite_stmt, 2, c_data, -1, SQLITE_STATIC);
		sqlite3_bind_int64(sqlite_stmt, 3, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 4, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 5, c_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

	} else {
		proceed = 9;
		/*EXEC_SQL UPDATE customer 
			SET c_balance = :c_balance
			WHERE c_w_id = :c_w_id 
			AND c_d_id = :c_d_id 
			AND c_id = :c_id;*/

		sqlite_stmt = stmt[t_num][18];

		sqlite3_bind_double(sqlite_stmt, 1, c_balance);
		sqlite3_bind_int64(sqlite_stmt, 2, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 3, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 4, c_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
	}

	strncpy(h_data, w_name, 10);
	h_data[10] = '\0';
	strncat(h_data, d_name, 10);
	h_data[20] = ' ';
	h_data[21] = ' ';
	h_data[22] = ' ';
	h_data[23] = ' ';
	h_data[24] = '\0';

	proceed = 10;
	/*EXEC_SQL INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
			                   h_w_id, h_date, h_amount, h_data)
	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
		               :w_id, 
			       :datetime,
			       :h_amount, :h_data);*/

	sqlite_stmt = stmt[t_num][19];

	sqlite3_bind_int64(sqlite_stmt, 1, c_d_id);
	sqlite3_bind_int64(sqlite_stmt, 2, c_w_id);
	sqlite3_bind_int64(sqlite_stmt, 3, c_id);
	sqlite3_bind_int64(sqlite_stmt, 4, d_id);
	sqlite3_bind_int64(sqlite_stmt, 5, w_id);
	sqlite3_bind_text(sqlite_stmt, 6, datetime, -1, SQLITE_STATIC);
	sqlite3_bind_double(sqlite_stmt, 7, h_amount);
	sqlite3_bind_text(sqlite_stmt, 8, h_data, -1, SQLITE_STATIC);

	if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

	sqlite3_reset(sqlite_stmt);

	/*EXEC_SQL COMMIT WORK;*/
	//if( sqlite3_exec(ctx[t_num], "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;
	return (1);

sqlerr:
        fprintf(stderr, "payment %d:%d\n",t_num,proceed);
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));
	exit(-1);
	//error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
sqlerrerr:
	return (0);
}
