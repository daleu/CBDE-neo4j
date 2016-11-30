from neo4j.v1 import GraphDatabase, basic_auth
import datetime

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "abcd"))
session = driver.session()

date1 = datetime.datetime(2016, 11, 11)
date2 = datetime.datetime(2016, 11, 12)
integerdat1 = str(10000*date1.year+100*date1.month+date1.day)
integerdat2 = str(10000*date2.year+100*date2.month+date2.day)

result = session.run("MATCH (a:Order)-[item:Lineitem]->(b:Partsupp) WHERE item.l_shipdate = 20161111 RETURN item.l_returnflag, item.l_linestatus, SUM(item.l_quantity) AS sum_qty, SUM(item.l_extended_price) AS sum_base_price, SUM(item.l_extended_price*(1-item.l_discount)) AS sum_disc_price, SUM(item.l_extended_price*(1-item.l_discount)*(1+item.l_tax)) AS sum_charge, AVG(item.l_quantity) AS avg_qty, AVG(item.l_extended_price) AS avg_price, AVG(item.l_discount) AS avg_disc, COUNT(*) AS count_order ORDER BY item.l_returnflag, item.l_linestatus DESC")

for record in result:
    print("%s" % (record))
    print("")

session.close()
