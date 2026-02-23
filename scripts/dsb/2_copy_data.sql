-- tables = ['call_center',
--    'catalog_page', 'catalog_returns',
--    'catalog_sales',
--    'customer', 'customer_address', 'customer_demographics',
--    'date_dim', 'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason', 'ship_mode',
--    'store', 'store_returns', 'store_sales',
--    'time_dim', 'warehouse',
--    'web_page', 'web_returns', 'web_sales', 'web_site'
-- ]


COPY call_center FROM '/tmp/data/call_center.dat' DELIMITER '|' CSV HEADER;
COPY catalog_page FROM '/tmp/data/catalog_page.dat' DELIMITER '|' CSV HEADER;
COPY catalog_sales FROM '/tmp/data/catalog_sales.dat' DELIMITER '|' CSV HEADER;
COPY customer FROM '/tmp/data/customer.dat' DELIMITER '|' CSV HEADER;
COPY customer_address FROM '/tmp/data/customer_address.dat' DELIMITER '|' CSV HEADER;
COPY customer_demographics FROM '/tmp/data/customer_demographics.dat' DELIMITER '|' CSV HEADER;
COPY date_dim FROM '/tmp/data/date_dim.dat' DELIMITER '|' CSV HEADER;
COPY household_demographics FROM '/tmp/data/household_demographics.dat' DELIMITER '|' CSV HEADER;
COPY income_band FROM '/tmp/data/income_band.dat' DELIMITER '|' CSV HEADER;
COPY inventory FROM '/tmp/data/inventory.dat' DELIMITER '|' CSV HEADER;
COPY item FROM '/tmp/data/item.dat' DELIMITER '|' CSV HEADER;
COPY promotion FROM '/tmp/data/promotion.dat' DELIMITER '|' CSV HEADER;
COPY reason FROM '/tmp/data/reason.dat' DELIMITER '|' CSV HEADER;
COPY ship_mode FROM '/tmp/data/ship_mode.dat' DELIMITER '|' CSV HEADER;
COPY store FROM '/tmp/data/store.dat' DELIMITER '|' CSV HEADER;
COPY store_returns FROM '/tmp/data/store_returns.dat' DELIMITER '|' CSV HEADER;
COPY store_sales FROM '/tmp/data/store_sales.dat' DELIMITER '|' CSV HEADER;
COPY time_dim FROM '/tmp/data/time_dim.dat' DELIMITER '|' CSV HEADER;
COPY warehouse FROM '/tmp/data/warehouse.dat' DELIMITER '|' CSV HEADER;
COPY web_page FROM '/tmp/data/web_page.dat' DELIMITER '|' CSV HEADER;
COPY web_returns FROM '/tmp/data/web_returns.dat' DELIMITER '|' CSV HEADER;
COPY web_sales FROM '/tmp/data/web_sales.dat' DELIMITER '|' CSV HEADER;
COPY web_site FROM '/tmp/data/web_site.dat' DELIMITER '|' CSV HEADER;


