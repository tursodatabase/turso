/*
 * -*-C-*-
 * slev.pc 
 * corresponds to A.5 in appendix A
 */

#include <string.h>
#include <stdio.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

extern sqlite3 **ctx;
extern sqlite3_stmt ***stmt;

/*
 * the stock level transaction
 */
int slev( int t_num,
	  int w_id_arg,		/* warehouse id */
	  int d_id_arg,		/* district id */
	  int level_arg		/* stock level */
)
{
	int ret;
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            level = level_arg;
	int            d_next_o_id;
	int            i_count;
	int            ol_i_id;

	sqlite3_stmt *sqlite_stmt;
	sqlite3_stmt *sqlite_stmt2;
	int num_cols;
	
	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/* find the next order id */
#ifdef DEBUG
	printf("select 1\n");
#endif
	/*EXEC_SQL SELECT d_next_o_id
	                INTO :d_next_o_id
	                FROM district
	                WHERE d_id = :d_id
			AND d_w_id = :w_id;*/
	sqlite_stmt = stmt[t_num][32];

	sqlite3_bind_int64(sqlite_stmt, 1, d_id);
	sqlite3_bind_int64(sqlite_stmt, 2, w_id);

	ret = sqlite3_step(sqlite_stmt);
	if (ret != SQLITE_DONE) {
		if (ret != SQLITE_ROW) goto sqlerr;
		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 1) goto sqlerr;

		d_next_o_id = sqlite3_column_int64(sqlite_stmt, 0);
	}

	sqlite3_reset(sqlite_stmt);

	/* find the most recent 20 orders for this district */
	/*EXEC_SQL DECLARE ord_line CURSOR FOR
	                SELECT DISTINCT ol_i_id
	                FROM order_line
	                WHERE ol_w_id = :w_id
			AND ol_d_id = :d_id
			AND ol_o_id < :d_next_o_id
			AND ol_o_id >= (:d_next_o_id - 20);

	EXEC_SQL OPEN ord_line;

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/
	sqlite_stmt = stmt[t_num][33];

	sqlite3_bind_int64(sqlite_stmt, 1, d_id);
	sqlite3_bind_int64(sqlite_stmt, 2, w_id);
	sqlite3_bind_int64(sqlite_stmt, 3, d_next_o_id);
	sqlite3_bind_int64(sqlite_stmt, 4, d_next_o_id);

	while (sqlite3_step(sqlite_stmt) != SQLITE_DONE) {

		num_cols = sqlite3_column_count(sqlite_stmt);
		if (num_cols != 1) goto sqlerr;
		ol_i_id = sqlite3_column_int64(sqlite_stmt, 0);

		/*EXEC_SQL SELECT count(*) INTO :i_count
			FROM stock
			WHERE s_w_id = :w_id
		        AND s_i_id = :ol_i_id
			AND s_quantity < :level;*/
		sqlite_stmt2 = stmt[t_num][34];

		sqlite3_bind_int64(sqlite_stmt, 1, w_id);
		sqlite3_bind_int64(sqlite_stmt, 2, ol_i_id);
		sqlite3_bind_int64(sqlite_stmt, 3, level);

		ret = sqlite3_step(sqlite_stmt2);
		if (ret != SQLITE_DONE) {
			if (ret != SQLITE_ROW) goto sqlerr;
			num_cols = sqlite3_column_count(sqlite_stmt2);
			if (num_cols != 1) goto sqlerr;
			i_count = sqlite3_column_int64(sqlite_stmt2, 0);
		}

		sqlite3_reset(sqlite_stmt2);

	}

	sqlite3_reset(sqlite_stmt);

done:
	/*EXEC_SQL CLOSE ord_line;*/
	/*EXEC_SQL COMMIT WORK;*/
	//if( sqlite3_exec(ctx[t_num], "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;
	return (1);

sqlerr:
        fprintf(stderr,"slev\n");
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));
	//error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
	return (0);

sqlerr2:
	fprintf(stderr,"slev\n");
	printf("%s: error: %s\n", __func__, sqlite3_errmsg(ctx[t_num]));
	//error(ctx[t_num],mysql_stmt2);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	//mysql_stmt_free_result(mysql_stmt);
	//mysql_rollback(ctx[t_num]);
	sqlite3_exec(ctx[t_num], "ROLLBACK;", NULL, NULL, NULL);
	return (0);
}
