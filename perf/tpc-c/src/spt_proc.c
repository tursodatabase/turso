/*
 * spt_proc.pc
 * support routines for the proc tpcc implementation
 */

#include <sqlite3.h>

#include <stdio.h>

/*
 * report error
 */
int error(
	  sqlite3 *sqlite,
	  sqlite3_stmt *sqlite_stmt
)
{
/*
	if(mysql_stmt) {
	    printf("\n%d, %s, %s", mysql_stmt_errno(mysql_stmt),
		   mysql_stmt_sqlstate(mysql_stmt), mysql_stmt_error(mysql_stmt) );
	}
*/
	if(sqlite){
		printf("%s: error!\n", __func__);
	}
	return (0);
}


