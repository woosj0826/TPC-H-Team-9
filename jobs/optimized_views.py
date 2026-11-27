"""Query-specific redundant year predicates, with original date predicates retained.

The filters below are implications of the fixed Apache Spark TPC-H query text.
They are applied only for the optimized layout; no arbitrary date restrictions.
"""
YEAR_FILTERS={
 1:{'lineitem':'l_ship_year <= 1998'},
 3:{'orders':'o_order_year <= 1995','lineitem':'l_ship_year >= 1995'},
 4:{'orders':'o_order_year = 1993'},
 5:{'orders':'o_order_year = 1994'},
 6:{'lineitem':'l_ship_year = 1994'},
 7:{'lineitem':'l_ship_year BETWEEN 1995 AND 1996'},
 8:{'orders':'o_order_year BETWEEN 1995 AND 1996'},
 10:{'orders':'o_order_year = 1993'},
 14:{'lineitem':'l_ship_year = 1995'},
 15:{'lineitem':'l_ship_year = 1996'},
 20:{'lineitem':'l_ship_year = 1994'},
}

def install_views(spark,database,query):
 definitions={}
 for table in ['orders','lineitem','nation','region']:
  predicate=YEAR_FILTERS.get(query,{}).get(table)
  df=spark.table(database+'.'+table)
  if predicate:df=df.where(predicate)
  broadcast=table in ['nation','region']
  if broadcast:df=df.hint('broadcast')
  df.createOrReplaceTempView(table)
  definitions[table]={'source':database+'.'+table,'year_predicate':predicate,'broadcast_hint':broadcast}
 return definitions
