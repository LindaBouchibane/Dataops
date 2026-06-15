import duckdb

con = duckdb.connect("warehouse.duckdb")

print(con.execute("DESCRIBE staging.orders").fetchdf())
print(con.execute("DESCRIBE staging.order_items").fetchdf())
print(con.execute("DESCRIBE staging.customers").fetchdf())
print(con.execute("DESCRIBE staging.products").fetchdf())

con.close()
