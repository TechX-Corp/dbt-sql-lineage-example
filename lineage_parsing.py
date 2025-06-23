from sqllineage.runner import LineageRunner

default_args = dict(
    dialect="redshift",
)

with open("./example_sql_code/with_cte.sql", "r") as f:
    sql_code = f.read()
    # For normal SQL, append the output like INSERT INTO output {sql_code}
    # For CTE SQL, put the INSERT statements after the CTE definition
    # if "WITH" in sql_code:
    #     sql_code = sql_code.replace("WITH", "WITH output AS ", 1)
    sql_code = f"INSERT INTO output {sql_code}"

result = LineageRunner(sql=sql_code, **default_args)

print(result)

for cols in result.get_column_lineage():
    print(cols)
