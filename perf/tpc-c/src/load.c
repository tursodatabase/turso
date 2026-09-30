/*
 * corresponds to A.6 in appendix A
 */

/*
 * ==================================================================+ | Load
 * TPCC tables
 * +==================================================================
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>

#include <sqlite3.h>

#include "spt_proc.h"
#include "tpc.h"

#define NNULL ((void *)0)
//#undef NULL

sqlite3* sqlite;
sqlite3_stmt* stmt[11];

/* Global SQL Variables */
char            timestamp[81];
long            count_ware;
int             fd, seed;

int             particle_flg = 0; /* "1" means particle mode */
int             part_no = 0; /* 1:items 2:warehouse 3:customer 4:orders */
long            min_ware = 1;
long            max_ware;

/* Global Variables */
int             i;
int             option_debug = 0;	/* 1 if generating debug output    */
int             is_local = 1;           /* "1" mean local */

#define DB_STRING_MAX 51

int
try_stmt_execute(sqlite3_stmt *sqlite_stmt)
{
    int ret = sqlite3_step(sqlite_stmt);
    if (ret != SQLITE_DONE) {
	    //printf("\n%d, %s, %s\n", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql) );
	printf("%s: error in executing statement\n", __func__);
        //mysql_rollback(mysql);
	sqlite3_exec(sqlite, "ROLLBACK;", NULL, NULL, NULL);

    }
    return ret;
}

/*
 * ==================================================================+ |
 * main() | ARGUMENTS |      Warehouses n [Debug] [Help]
 * +==================================================================
 */
void 
main(argc, argv)
	int             argc;
	char           *argv[];
{
	char            arg[2];
        char           *ptr;

	int i,c;

	sqlite3* resp;
	
	/* initialize */
	count_ware = 0;

	printf("*************************************\n");
	printf("*** TPCC-sqlite3 Data Loader        ***\n");
	printf("*************************************\n");

  /* Parse args */

    while ( (c = getopt(argc, argv, "w:l:m:n:")) != -1) {
        switch (c) {
        case 'w':
            printf ("option w with value '%s'\n", optarg);
            count_ware = atoi(optarg);
            break;
        case 'l':
            printf ("option l with value '%s'\n", optarg);
            part_no = atoi(optarg);
	    particle_flg = 1;
            break;
        case 'm':
            printf ("option m with value '%s'\n", optarg);
            min_ware = atoi(optarg);
            break;
        case 'n':
            printf ("option n with value '%s'\n", optarg);
            max_ware = atoi(optarg);
            break;
        case '?':
    	    printf("Usage: tpcc_load -w warehouses -m min_wh -n max_wh\n");
    	    printf("* [part]: 1=ITEMS 2=WAREHOUSE 3=CUSTOMER 4=ORDERS\n");
            exit(0);
        default:
            printf ("?? getopt returned character code 0%o ??\n", c);
        }
    }
    if (optind < argc) {
        printf ("non-option ARGV-elements: ");
        while (optind < argc)
            printf ("%s ", argv[optind++]);
        printf ("\n");
    }

    if(particle_flg==0){
	    min_ware = 1;
	    max_ware = count_ware;
    }

    if(particle_flg==1){
	    printf("  [part(1-4)]: %d\n", part_no);
	    printf("     [MIN WH]: %d\n", min_ware);
	    printf("     [MAX WH]: %d\n", max_ware);
    }

    fd = open("/dev/urandom", O_RDONLY);
    if (fd == -1) {
	    fd = open("/dev/random", O_RDONLY);
	    if (fd == -1) {
		    struct timeval  tv;
		    gettimeofday(&tv, NNULL);
		    seed = (tv.tv_sec ^ tv.tv_usec) * tv.tv_sec * tv.tv_usec ^ tv.tv_sec;
	    }else{
		    read(fd, &seed, sizeof(seed));
		    close(fd);
	    }
    }else{
	    read(fd, &seed, sizeof(seed));
	    close(fd);
    }
    SetSeed(seed);

    /* Initialize timestamp (for date columns) */
    gettimestamp(timestamp, STRFTIME_FORMAT, TIMESTAMP_LEN);

    /* EXEC SQL WHENEVER SQLERROR GOTO Error_SqlCall; */

    sqlite3_open(DB_PATH, &sqlite);
    if(!sqlite) {
	    printf("%s: Failed to open DB\n", __func__);
    }

    /*
    for( i=0; i<11; i++ ){
	    stmt[i] = mysql_stmt_init(mysql);
	    if(!stmt[i]) goto Error_SqlCall_close;
    }
    */
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO item values(?,?,?,?,?)",
			   -1, &stmt[0], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO warehouse values(?,?,?,?,?,?,?,?,?)",
			   -1, &stmt[1], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO stock values(?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?)",
			   -1, &stmt[2], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO district values(?,?,?,?,?,?,?,?,?,?,?)",
			   -1, &stmt[3], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO customer values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 10.0, 1, 0,?)",
			   -1, &stmt[4], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO history values(?,?,?,?,?,?,?,?)",
			   -1, &stmt[5], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO orders values(?,?,?,?,?,NULL,?, 1)",
			   -1, &stmt[6], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO new_orders values(?,?,?)",
			   -1, &stmt[7], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO orders values(?,?,?,?,?,?,?, 1)",
			   -1, &stmt[8], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO order_line values(?,?,?,?,?,?, NULL,?,?,?)",
			   -1, &stmt[9], NULL) != SQLITE_OK) goto Error_SqlCall_close;
    if( sqlite3_prepare_v2(sqlite,
			   "INSERT INTO order_line values(?,?,?,?,?,?,?,?,?,?)",
			   -1, &stmt[10], NULL) != SQLITE_OK) goto Error_SqlCall_close;


	/* exec sql begin transaction; */

	printf("TPCC Data Load Started...\n");
	//if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto Error_SqlCall;

	if(particle_flg==0){
	    LoadItems();
	    LoadWare();
	    LoadCust();
	    LoadOrd();
	}else if(particle_flg==1){
	    switch(part_no){
		case 1:
		    LoadItems();
		    break;
		case 2:
		    LoadWare();
		    break;
		case 3:
		    LoadCust();
		    break;
		case 4:
		    LoadOrd();
		    break;
		default:
		    printf("Unknown part_no\n");
		    printf("1:ITEMS 2:WAREHOUSE 3:CUSTOMER 4:ORDERS\n");
	    }
	}

	/* EXEC SQL COMMIT WORK; */

	//if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto Error_SqlCall;

	for( i=0; i<11; i++ ){
		sqlite3_reset(stmt[i]);
	}

	/* EXEC SQL DISCONNECT; */

	sqlite3_close(sqlite);
	
	printf("\n...DATA LOADING COMPLETED SUCCESSFULLY.\n");
	exit(0);
Error_SqlCall_close:
Error_SqlCall:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadItems | DESCRIPTION |      Loads the Item table |
 * ARGUMENTS |      none
 * +==================================================================
 */
void 
LoadItems()
{

	int ret;
	int             i_id;
	int             i_im_id;
        char            i_name[25];
	float           i_price;
	char            i_data[51];

	int             idatasiz;
	int             orig[MAXITEMS+1];
	int             pos;
	int             i;
	int             retried = 0;
	sqlite3_stmt* sqlite_stmt;
	
	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Item \n");

	for (i = 0; i < MAXITEMS / 10; i++)
		orig[i] = 0;
	for (i = 0; i < MAXITEMS / 10; i++) {
		do {
			pos = RandomNumber(0L, MAXITEMS);
		} while (orig[pos]);
		orig[pos] = 1;
	}
retry:
    if (retried)
        printf("Retrying ...\n");
    retried = 1;

    if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

    for (i_id = 1; i_id <= MAXITEMS; i_id++) {

		/* Generate Item Data */
		i_im_id = RandomNumber(1L, 10000L);

                i_name[ MakeAlphaString(14, 24, i_name) ] = 0;

		i_price = ((int) RandomNumber(100L, 10000L)) / 100.0;

		idatasiz = MakeAlphaString(26, 50, i_data);
		i_data[idatasiz] = 0;

		if (orig[i_id]) {
			pos = RandomNumber(0L, idatasiz - 8);
			i_data[pos] = 'o';
			i_data[pos + 1] = 'r';
			i_data[pos + 2] = 'i';
			i_data[pos + 3] = 'g';
			i_data[pos + 4] = 'i';
			i_data[pos + 5] = 'n';
			i_data[pos + 6] = 'a';
			i_data[pos + 7] = 'l';
		}
		if (option_debug)
			printf("IID = %ld, Name= %16s, Price = %5.2f\n",
			       i_id, i_name, i_price);

#if 0
		printf("about to exec sql\n");
		fflush(stdout);
#endif

		/* EXEC SQL INSERT INTO
		                item
		                values(:i_id,:i_im_id,:i_name,:i_price,:i_data); */

		sqlite_stmt = stmt[0];
		
		//printf("%s: inserting item id = %d\n", __func__, i_id);
		sqlite3_bind_int64(sqlite_stmt, 1, i_id);
		sqlite3_bind_int64(sqlite_stmt, 2, i_im_id);
		sqlite3_bind_text(sqlite_stmt, 3, i_name, -1, SQLITE_STATIC);
		sqlite3_bind_double(sqlite_stmt, 4, i_price);
		sqlite3_bind_text(sqlite_stmt, 5, i_data, -1,  SQLITE_STATIC);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);

#if 0
		printf("done executing sql\n");
		fflush(stdout);
#endif

		if (!(i_id % 100)) {
			printf(".");
			fflush(stdout);

			if (!(i_id % 5000))
				printf(" %ld\n", i_id);
		}
	}

	/* EXEC SQL COMMIT WORK; */
	if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	printf("Item Done. \n");
	return;
sqlerr:
	Error(stmt[0]);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadWare | DESCRIPTION |      Loads the Warehouse
 * table |      Loads Stock, District as Warehouses are created | ARGUMENTS |
 * none +==================================================================
 */
void 
LoadWare()
{

	int             w_id;
        char            w_name[11];
        char            w_street_1[21];
        char            w_street_2[21];
        char            w_city[21];
        char            w_state[3];
        char            w_zip[10];
	float           w_tax;
	float           w_ytd;

	int             tmp;
	int             retried = 0;
	sqlite3_stmt* sqlite_stmt;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Warehouse \n");
	w_id = min_ware;
 retry:
	if (retried)
		printf("Retrying ....\n");
	retried = 1;

	for (; w_id <= max_ware; w_id++) {

		if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;
		/* Generate Warehouse Data */

                w_name[ MakeAlphaString(6, 10, w_name) ] = 0;

		MakeAddress(w_street_1, w_street_2, w_city, w_state, w_zip);

		w_tax = ((float) RandomNumber(10L, 20L)) / 100.0;
		w_ytd = 300000.00;

		if (option_debug)
			printf("WID = %ld, Name= %16s, Tax = %5.2f\n",
			       w_id, w_name, w_tax);

		/*EXEC SQL INSERT INTO
		                warehouse
		                values(:w_id,:w_name,
				       :w_street_1,:w_street_2,:w_city,:w_state,
				       :w_zip,:w_tax,:w_ytd);*/

		sqlite_stmt = stmt[1];

		sqlite3_bind_int64(sqlite_stmt, 1, w_id);
		sqlite3_bind_text(sqlite_stmt, 2, w_name, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 3, w_street_1, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 4, w_street_2, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 5, w_city, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 6, w_state, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 7, w_zip, -1, SQLITE_STATIC);
		sqlite3_bind_double(sqlite_stmt, 8, w_tax);
		sqlite3_bind_double(sqlite_stmt, 9, w_ytd);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;
		
		/** Make Rows associated with Warehouse **/
		if( Stock(w_id) ) goto retry;
		if( District(w_id) ) goto retry;

		sqlite3_reset(sqlite_stmt);

		/* EXEC SQL COMMIT WORK; */
		if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	}

	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadCust | DESCRIPTION |      Loads the Customer Table
 * | ARGUMENTS |      none
 * +==================================================================
 */
void 
LoadCust()
{

	int             w_id;
	int             d_id;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	for (w_id = min_ware; w_id <= max_ware; w_id++)
		for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++)
			Customer(d_id, w_id);

	/* EXEC SQL COMMIT WORK;*/	/* Just in case */
	if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      LoadOrd | DESCRIPTION |      Loads the Orders and
 * Order_Line Tables | ARGUMENTS |      none
 * +==================================================================
 */
void 
LoadOrd()
{

	int             w_id;
	float           w_tax;
	int             d_id;
	float           d_tax;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/
	if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	for (w_id = min_ware; w_id <= max_ware; w_id++)
		for (d_id = 1L; d_id <= DIST_PER_WARE; d_id++)
			Orders(d_id, w_id);

	/* EXEC SQL COMMIT WORK; */	/* Just in case */
	if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Stock | DESCRIPTION |      Loads the Stock table |
 * ARGUMENTS |      w_id - warehouse id
 * +==================================================================
 */
int 
Stock(w_id)
	int             w_id;
{

	int             s_i_id;
	int             s_w_id;
	int             s_quantity;

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
	char            s_data[51];

	int             sdatasiz;
	int             orig[MAXITEMS+1];
	int             pos;
	int             i;
	int             error;
	sqlite3_stmt* sqlite_stmt;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/
	printf("Loading Stock Wid=%ld\n", w_id);
	s_w_id = w_id;

	for (i = 0; i < MAXITEMS / 10; i++)
		orig[i] = 0;
	for (i = 0; i < MAXITEMS / 10; i++) {
		do {
			pos = RandomNumber(0L, MAXITEMS);
		} while (orig[pos]);
		orig[pos] = 1;
	}

retry:
	for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id++) {

		/* Generate Stock Data */
		s_quantity = RandomNumber(10L, 100L);

		s_dist_01[ MakeAlphaString(24, 24, s_dist_01) ] = 0;
		s_dist_02[ MakeAlphaString(24, 24, s_dist_02) ] = 0;
		s_dist_03[ MakeAlphaString(24, 24, s_dist_03) ] = 0;
		s_dist_04[ MakeAlphaString(24, 24, s_dist_04) ] = 0;
		s_dist_05[ MakeAlphaString(24, 24, s_dist_05) ] = 0;
		s_dist_06[ MakeAlphaString(24, 24, s_dist_06) ] = 0;
		s_dist_07[ MakeAlphaString(24, 24, s_dist_07) ] = 0;
		s_dist_08[ MakeAlphaString(24, 24, s_dist_08) ] = 0;
		s_dist_09[ MakeAlphaString(24, 24, s_dist_09) ] = 0;
		s_dist_10[ MakeAlphaString(24, 24, s_dist_10) ] = 0;
		sdatasiz = MakeAlphaString(26, 50, s_data);
		s_data[sdatasiz] = 0;

		if (orig[s_i_id]) {
			pos = RandomNumber(0L, sdatasiz - 8);

			s_data[pos] = 'o';
			s_data[pos + 1] = 'r';
			s_data[pos + 2] = 'i';
			s_data[pos + 3] = 'g';
			s_data[pos + 4] = 'i';
			s_data[pos + 5] = 'n';
			s_data[pos + 6] = 'a';
			s_data[pos + 7] = 'l';

		}
		/*EXEC SQL INSERT INTO
		                stock
		                values(:s_i_id,:s_w_id,:s_quantity,
				       :s_dist_01,:s_dist_02,:s_dist_03,:s_dist_04,:s_dist_05,
				       :s_dist_06,:s_dist_07,:s_dist_08,:s_dist_09,:s_dist_10,
				       0, 0, 0,:s_data);*/

		sqlite_stmt = stmt[2];

		sqlite3_bind_int64(sqlite_stmt, 1, s_i_id);
		sqlite3_bind_int64(sqlite_stmt, 2, s_w_id);
		sqlite3_bind_int64(sqlite_stmt, 3, s_quantity);
		sqlite3_bind_text(sqlite_stmt, 4, s_dist_01, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 5, s_dist_02, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 6, s_dist_03, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 7, s_dist_04, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 8, s_dist_05, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 9, s_dist_06, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 10, s_dist_07, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 11, s_dist_08, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 12, s_dist_09, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 13, s_dist_10, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 14, s_data, -1, SQLITE_STATIC);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
		
		if (option_debug)
			printf("SID = %ld, WID = %ld, Quan = %ld\n",
			       s_i_id, s_w_id, s_quantity);

		if (!(s_i_id % 100)) {
			printf(".");
			fflush(stdout);
			if (!(s_i_id % 5000))
				printf(" %ld\n", s_i_id);
		}
	}

	printf(" Stock Done.\n");
out:
	return error;
sqlerr:
    Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      District | DESCRIPTION |      Loads the District table
 * | ARGUMENTS |      w_id - warehouse id
 * +==================================================================
 */
int 
District(w_id)
	int             w_id;
{

	int             d_id;
	int             d_w_id;

	char            d_name[11];
	char            d_street_1[21];
	char            d_street_2[21];
	char            d_city[21];
	char            d_state[3];
	char            d_zip[10];

	float           d_tax;
	float           d_ytd;
	int             d_next_o_id;
	int             error;
	sqlite3_stmt* sqlite_stmt;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	printf("Loading District\n");
	d_w_id = w_id;
	d_ytd = 30000.0;
	d_next_o_id = 3001L;
retry:
	for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {

		/* Generate District Data */

		d_name[ MakeAlphaString(6L, 10L, d_name) ] = 0;
		MakeAddress(d_street_1, d_street_2, d_city, d_state, d_zip);

		d_tax = ((float) RandomNumber(10L, 20L)) / 100.0;

		/*EXEC SQL INSERT INTO
		                district
		                values(:d_id,:d_w_id,:d_name,
				       :d_street_1,:d_street_2,:d_city,:d_state,:d_zip,
				       :d_tax,:d_ytd,:d_next_o_id);*/

		sqlite_stmt = stmt[3];

		sqlite3_bind_int64(sqlite_stmt, 1, d_id);
		sqlite3_bind_int64(sqlite_stmt, 2, d_w_id);
		sqlite3_bind_text(sqlite_stmt, 3, d_name, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 4, d_street_1, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 5, d_street_2, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 6, d_city, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 7, d_state, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 8, d_zip, -1, SQLITE_STATIC);
		sqlite3_bind_double(sqlite_stmt, 9, d_tax);
		sqlite3_bind_double(sqlite_stmt, 10, d_ytd);
		sqlite3_bind_int64(sqlite_stmt, 11, d_next_o_id);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
		
		if (option_debug)
			printf("DID = %ld, WID = %ld, Name = %10s, Tax = %5.2f\n",
			       d_id, d_w_id, d_name, d_tax);

	}
	
	printf(" Stock Done.\n");
out:
	return error;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Customer | DESCRIPTION |      Loads Customer Table |
 * Also inserts corresponding history record | ARGUMENTS |      id   -
 * customer id |      d_id - district id |      w_id - warehouse id
 * +==================================================================
 */
void 
Customer(d_id, w_id)
	int             d_id;
	int             w_id;
{
	int             c_id;
	int             c_d_id;
	int             c_w_id;

	char            c_first[17];
	char            c_middle[3];
	char            c_last[17];
	char            c_street_1[21];
	char            c_street_2[21];
	char            c_city[21];
	char            c_state[3];
	char            c_zip[10];
	char            c_phone[17];
	char            c_since[12];
	char            c_credit[3];

	int             c_credit_lim;
	float           c_discount;
	float           c_balance;
	char            c_data[501];

	float           h_amount;

	char            h_data[25];
	int             retried = 0;
	sqlite3_stmt* sqlite_stmt;

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	printf("Loading Customer for DID=%ld, WID=%ld\n", d_id, w_id);

retry:
    if (retried)
        printf("Retrying ...\n");
    retried = 1;

    //if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

    for (c_id = 1; c_id <= CUST_PER_DIST; c_id++) {

		/* Generate Customer Data */
		c_d_id = d_id;
		c_w_id = w_id;

		c_first[ MakeAlphaString(8, 16, c_first) ] = 0;
		c_middle[0] = 'O';
		c_middle[1] = 'E';
		c_middle[2] = 0;

		if (c_id <= 1000) {
			Lastname(c_id - 1, c_last);
		} else {
			Lastname(NURand(255, 0, 999), c_last);
		}

		MakeAddress(c_street_1, c_street_2, c_city, c_state, c_zip);
		c_phone[ MakeNumberString(16, 16, c_phone) ] = 0;

		if (RandomNumber(0L, 1L))
			c_credit[0] = 'G';
		else
			c_credit[0] = 'B';
		c_credit[1] = 'C';
		c_credit[2] = 0;

		c_credit_lim = 50000;
		c_discount = ((float) RandomNumber(0L, 50L)) / 100.0;
		c_balance = -10.0;

		c_data[ MakeAlphaString(300, 500, c_data) ] = 0;

		/*EXEC SQL INSERT INTO
		                customer
		                values(:c_id,:c_d_id,:c_w_id,
				  :c_first,:c_middle,:c_last,
				  :c_street_1,:c_street_2,:c_city,:c_state,
				  :c_zip,
			          :c_phone, :timestamp,
				  :c_credit,
				  :c_credit_lim,:c_discount,:c_balance,
				  10.0, 1, 0,:c_data);*/


		sqlite_stmt = stmt[4];

		sqlite3_bind_int64(sqlite_stmt, 1, c_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, c_w_id);
		sqlite3_bind_text(sqlite_stmt, 4, c_first, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 5, c_middle, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 6, c_last, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 7, c_street_1, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 8, c_street_2, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 9, c_city, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 10, c_state, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 11, c_zip, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 12, c_phone, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 13, timestamp, -1, SQLITE_STATIC);
		sqlite3_bind_text(sqlite_stmt, 14, c_credit, -1, SQLITE_STATIC);
		sqlite3_bind_int64(sqlite_stmt, 15, c_credit_lim);
		sqlite3_bind_double(sqlite_stmt, 16, c_discount);		
		sqlite3_bind_double(sqlite_stmt, 17, c_balance);
		sqlite3_bind_text(sqlite_stmt, 18, c_data, -1, SQLITE_STATIC);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
		
		h_amount = 10.0;

		h_data[ MakeAlphaString(12, 24, h_data) ] = 0;

		/*EXEC SQL INSERT INTO
		                history
		                values(:c_id,:c_d_id,:c_w_id,
				       :c_d_id,:c_w_id, :timestamp,
				       :h_amount,:h_data);*/
		
		sqlite_stmt = stmt[5];

		sqlite3_bind_int64(sqlite_stmt, 1, c_id);
		sqlite3_bind_int64(sqlite_stmt, 2, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 3, c_w_id);
		sqlite3_bind_int64(sqlite_stmt, 4, c_d_id);
		sqlite3_bind_int64(sqlite_stmt, 5, c_w_id);
		sqlite3_bind_text(sqlite_stmt, 6, timestamp, -1, SQLITE_STATIC);
		sqlite3_bind_double(sqlite_stmt, 7, h_amount);		
		sqlite3_bind_text(sqlite_stmt, 8, h_data, -1, SQLITE_STATIC);

		if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

		sqlite3_reset(sqlite_stmt);
		
		if (option_debug)
			printf("CID = %ld, LST = %s, P# = %s\n",
			       c_id, c_last, c_phone);
		if (!(c_id % 100)) {
 			printf(".");
			fflush(stdout);
			if (!(c_id % 1000))
				printf(" %ld\n", c_id);
		}
    }
    /* EXEC SQL COMMIT WORK; */
    //if( mysql_commit(mysql) ) goto sqlerr;
    //if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;
    printf("Customer Done.\n");

    return;
sqlerr:
    Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Orders | DESCRIPTION |      Loads the Orders table |
 * Also loads the Order_Line table on the fly | ARGUMENTS |      w_id -
 * warehouse id
 * +==================================================================
 */
void 
Orders(d_id, w_id)
	int             d_id, w_id;
{

	int             o_id;
	int             o_c_id;
	int             o_d_id;
	int             o_w_id;
	int             o_carrier_id;
	int             o_ol_cnt;
	int             ol;
	int             ol_i_id;
	int             ol_supply_w_id;
	int             ol_quantity;
	float           ol_amount;
	char            ol_dist_info[25];
	float           i_price;
	float           c_discount;
	float           tmp_float;
	int             retried = 0;
	sqlite3_stmt* sqlite_stmt;

	/* EXEC SQL WHENEVER SQLERROR GOTO sqlerr; */

	printf("Loading Orders for D=%ld, W= %ld\n", d_id, w_id);
	o_d_id = d_id;
	o_w_id = w_id;
retry:
    if (retried)
        printf("Retrying ...\n");
    retried = 1;
	InitPermutation();	/* initialize permutation of customer numbers */

	//if( sqlite3_exec(sqlite, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	for (o_id = 1; o_id <= ORD_PER_DIST; o_id++) {

		/* Generate Order Data */
		o_c_id = GetPermutation();
		o_carrier_id = RandomNumber(1L, 10L);
		o_ol_cnt = RandomNumber(5L, 15L);

		if (o_id > 2100) {	/* the last 900 orders have not been
					 * delivered) */
		    /*EXEC SQL INSERT INTO
			                orders
			                values(:o_id,:o_d_id,:o_w_id,:o_c_id,
					       :timestamp,
					       NULL,:o_ol_cnt, 1);*/

			sqlite_stmt = stmt[6];

			sqlite3_bind_int64(sqlite_stmt, 1, o_id);
			sqlite3_bind_int64(sqlite_stmt, 2, o_d_id);
			sqlite3_bind_int64(sqlite_stmt, 3, o_w_id);
			sqlite3_bind_int64(sqlite_stmt, 4, o_c_id);
			sqlite3_bind_text(sqlite_stmt, 5, timestamp, -1, SQLITE_STATIC);
			sqlite3_bind_int64(sqlite_stmt, 6, o_ol_cnt);

			if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

			sqlite3_reset(sqlite_stmt);

			/*EXEC SQL INSERT INTO
			                new_orders
			                values(:o_id,:o_d_id,:o_w_id);*/

			sqlite_stmt = stmt[7];

			sqlite3_bind_int64(sqlite_stmt, 1, o_id);
			sqlite3_bind_int64(sqlite_stmt, 2, o_d_id);
			sqlite3_bind_int64(sqlite_stmt, 3, o_w_id);

			if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

			sqlite3_reset(sqlite_stmt);
			
		} else {
		    /*EXEC SQL INSERT INTO
			    orders
			    values(:o_id,:o_d_id,:o_w_id,:o_c_id,
				   :timestamp,
				   :o_carrier_id,:o_ol_cnt, 1);*/


			sqlite_stmt = stmt[8];

			sqlite3_bind_int64(sqlite_stmt, 1, o_id);
			sqlite3_bind_int64(sqlite_stmt, 2, o_d_id);
			sqlite3_bind_int64(sqlite_stmt, 3, o_w_id);
			sqlite3_bind_int64(sqlite_stmt, 4, o_c_id);
			sqlite3_bind_text(sqlite_stmt, 5, timestamp, -1, SQLITE_STATIC);
			sqlite3_bind_int64(sqlite_stmt, 6, o_carrier_id);
			sqlite3_bind_int64(sqlite_stmt, 7, o_ol_cnt);

			if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

			sqlite3_reset(sqlite_stmt);

		}

		if (option_debug)
			printf("OID = %ld, CID = %ld, DID = %ld, WID = %ld\n",
			       o_id, o_c_id, o_d_id, o_w_id);

		for (ol = 1; ol <= o_ol_cnt; ol++) {
			/* Generate Order Line Data */
			ol_i_id = RandomNumber(1L, MAXITEMS);
			ol_supply_w_id = o_w_id;
			ol_quantity = 5;
			ol_amount = 0.0;

			ol_dist_info[ MakeAlphaString(24, 24, ol_dist_info) ] = 0;

			tmp_float = (float) (RandomNumber(10L, 10000L)) / 100.0;

			if (o_id > 2100) {
			    /*EXEC SQL INSERT INTO
				                order_line
				                values(:o_id,:o_d_id,:o_w_id,:ol,
						       :ol_i_id,:ol_supply_w_id, NULL,
						       :ol_quantity,:tmp_float,:ol_dist_info);*/
				sqlite_stmt = stmt[9];

				sqlite3_bind_int64(sqlite_stmt, 1, o_id);
				sqlite3_bind_int64(sqlite_stmt, 2, o_d_id);
				sqlite3_bind_int64(sqlite_stmt, 3, o_w_id);
				sqlite3_bind_int64(sqlite_stmt, 4, ol);
				sqlite3_bind_int64(sqlite_stmt, 5, ol_i_id);
				sqlite3_bind_int64(sqlite_stmt, 6, ol_supply_w_id);
				sqlite3_bind_int64(sqlite_stmt, 7, ol_quantity);
				sqlite3_bind_double(sqlite_stmt, 8, tmp_float);
				sqlite3_bind_text(sqlite_stmt, 9, ol_dist_info, -1, SQLITE_STATIC);

				if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;

				sqlite3_reset(sqlite_stmt);
				
			} else {
			    /*EXEC SQL INSERT INTO
				    order_line
				    values(:o_id,:o_d_id,:o_w_id,:ol,
					   :ol_i_id,:ol_supply_w_id, 
					   :timestamp,
					   :ol_quantity,:ol_amount,:ol_dist_info);*/


				sqlite_stmt = stmt[10];

				sqlite3_bind_int64(sqlite_stmt, 1, o_id);
				sqlite3_bind_int64(sqlite_stmt, 2, o_d_id);
				sqlite3_bind_int64(sqlite_stmt, 3, o_w_id);
				sqlite3_bind_int64(sqlite_stmt, 4, ol);
				sqlite3_bind_int64(sqlite_stmt, 5, ol_i_id);
				sqlite3_bind_int64(sqlite_stmt, 6, ol_supply_w_id);
				sqlite3_bind_text(sqlite_stmt, 7, timestamp, -1, SQLITE_STATIC);
				sqlite3_bind_int64(sqlite_stmt, 8, ol_quantity);
				sqlite3_bind_double(sqlite_stmt, 9, ol_amount);
				sqlite3_bind_text(sqlite_stmt, 10, ol_dist_info, -1, SQLITE_STATIC);

				if (sqlite3_step(sqlite_stmt) != SQLITE_DONE) goto sqlerr;
				sqlite3_reset(sqlite_stmt);

			}

			if (option_debug)
				printf("OL = %ld, IID = %ld, QUAN = %ld, AMT = %8.2f\n",
				       ol, ol_i_id, ol_quantity, ol_amount);

		}
		if (!(o_id % 100)) {
			printf(".");
			fflush(stdout);

 			if (!(o_id % 1000))
				printf(" %ld\n", o_id);
		}
	}
	/*EXEC SQL COMMIT WORK;*/
	//if( sqlite3_exec(sqlite, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) goto sqlerr;

	printf("Orders Done.\n");
	return;
sqlerr:
	Error(0);
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      MakeAddress() | DESCRIPTION |      Build an Address |
 * ARGUMENTS
 * +==================================================================
 */
void 
MakeAddress(str1, str2, city, state, zip)
	char           *str1;
	char           *str2;
	char           *city;
	char           *state;
	char           *zip;
{
	str1[ MakeAlphaString(10, 20, str1) ] = 0;	/* Street 1 */
	str2[ MakeAlphaString(10, 20, str2) ] = 0;	/* Street 2 */
	city[ MakeAlphaString(10, 20, city) ] = 0;	/* City */
	state[ MakeAlphaString(2, 2, state) ] = 0;	/* State */
	zip[ MakeNumberString(9, 9, zip) ] = 0;	/* Zip */
}

/*
 * ==================================================================+ |
 * ROUTINE NAME |      Error() | DESCRIPTION |      Handles an error from a
 * SQL call. | ARGUMENTS
 * +==================================================================
 */
void 
Error(sqlite_stmt)
        sqlite3_stmt   *sqlite_stmt;
{
    if(sqlite_stmt) {
	    //printf("\n%d, %s, %s", mysql_stmt_errno(mysql_stmt),
	    //mysql_stmt_sqlstate(mysql_stmt), mysql_stmt_error(mysql_stmt) );
	    printf("%s: sqlite error: %s\n", __func__, sqlite3_errmsg(sqlite));
    }
    //printf("\n%d, %s, %s\n", mysql_errno(mysql), mysql_sqlstate(mysql), mysql_error(mysql) );
    printf("%s: sqlite error: %s\n", __func__, sqlite3_errmsg(sqlite));

    /*EXEC SQL WHENEVER SQLERROR CONTINUE;*/

    /*EXEC SQL ROLLBACK WORK;*/
    sqlite3_exec(sqlite, "ROLLBACK;", NULL, NULL, NULL);

    /*EXEC SQL DISCONNECT;*/
    sqlite3_close(sqlite);
    
    exit(-1);
}
