"""

"""

DIALECT = "postgres"


class PolarsSQL:

    def do_sql(self, inp: str):
        """
        Query a cached polars.DataFrame with an SQL query.
        """
        import polars as pl
        import sqlglot
        #from sqlglot.errors import ParseError, TokenError

        # Create an SQLContext
        ctx = pl.SQLContext(eager=True)

        # Get table names
        tree = sqlglot.parse_one(inp, dialect=DIALECT)
        cte_names = {c.alias_or_name for c in tree.find_all(sqlglot.exp.CTE)}
        tables = {
            t.name for t in tree.find_all(sqlglot.exp.Table)
            if t.name not in cte_names   # CTEs aren't real tables
        }

        # Populate the SQLContext
        _ = [ctx.register(name, self.cache.get(name).value) for name in tables]

        df = ctx.execute(inp)
        self.console.print(df)

        return

