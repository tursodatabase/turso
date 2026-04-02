CREATE INDEX IF NOT EXISTS idx_customer ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX IF NOT EXISTS idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);
CREATE INDEX IF NOT EXISTS fkey_stock_2 ON stock (s_i_id);
CREATE INDEX IF NOT EXISTS fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);
