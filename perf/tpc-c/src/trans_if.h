/*
 * trans_if.h
 *
 * prototypes for the transaction interface calls
 */

#ifdef __cplusplus
extern "C" {
#endif

int driver (int t_num);
int neword (int t_num, int w_id_arg, int d_id_arg, int c_id_arg,
	    int o_ol_cnt_arg, int o_all_local_arg, int itemid[],
	    int supware[], int qty[]);
int payment (int t_num, int w_id_arg, int d_id_arg, int byname,
	     int c_w_id_arg, int c_d_id_arg,
	     int c_id_arg, char c_last_arg[], float h_amount_arg);
int ordstat (int t_num, int w_id, int d_id, int byname, int c_id,
	     char c_last[]);
int slev (int t_num, int w_id, int d_id, int level);
int delivery (int t_num, int w_id_arg, int o_carrier_id_arg);

#ifdef __cplusplus
}
#endif
