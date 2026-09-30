drop table if exists warehouse;

create table warehouse (
w_id integer not null,
w_name text,
w_street_1 text,
w_street_2 text,
w_city text,
w_state text,
w_zip text,
w_tax real,
w_ytd real,
primary key (w_id));

drop table if exists district;

create table district (
d_id integer not null,
d_w_id integer not null,
d_name text,
d_street_1 text,
d_street_2 text,
d_city text,
d_state text,
d_zip text,
d_tax real,
d_ytd real,
d_next_o_id integer,
primary key (d_w_id, d_id));

drop table if exists customer;

create table customer (
c_id integer not null,
c_d_id integer not null,
c_w_id integer not null,
c_first text,
c_middle text,
c_last text,
c_street_1 text,
c_street_2 text,
c_city text,
c_state text,
c_zip text,
c_phone text,
c_since text,
c_credit text,
c_credit_lim integer,
c_discount real,
c_balance real,
c_ytd_payment real,
c_payment_cnt integer,
c_delivery_cnt integer,
c_data text,
primary key(c_w_id, c_d_id, c_id));

drop table if exists history;

create table history (
h_c_id integer,
h_c_d_id integer,
h_c_w_id integer,
h_d_id integer,
h_w_id integer,
h_date text,
h_amount real,
h_data text);

drop table if exists new_orders;

create table new_orders (
no_o_id integer not null,
no_d_id integer not null,
no_w_id integer not null,
primary key(no_w_id, no_d_id, no_o_id));

drop table if exists orders;

create table orders (
o_id integer not null,
o_d_id integer not null,
o_w_id integer not null,
o_c_id integer,
o_entry_d text,
o_carrier_id integer,
o_ol_cnt integer,
o_all_local integer,
primary key(o_w_id, o_d_id, o_id));

drop table if exists order_line;

create table order_line (
ol_o_id integer not null,
ol_d_id integer not null,
ol_w_id integer not null,
ol_number integer not null,
ol_i_id integer,
ol_supply_w_id integer,
ol_delivery_d text,
ol_quantity integer,
ol_amount real,
ol_dist_info text,
primary key(ol_w_id, ol_d_id, ol_o_id, ol_number));

drop table if exists item;

create table item (
i_id integer not null,
i_im_id integer,
i_name text,
i_price real,
i_data text,
primary key(i_id));

drop table if exists stock;

create table stock (
s_i_id integer not null,
s_w_id integer not null,
s_quantity integer,
s_dist_01 text,
s_dist_02 text,
s_dist_03 text,
s_dist_04 text,
s_dist_05 text,
s_dist_06 text,
s_dist_07 text,
s_dist_08 text,
s_dist_09 text,
s_dist_10 text,
s_ytd real,
s_order_cnt integer,
s_remote_cnt integer,
s_data text,
primary key(s_w_id, s_i_id));
