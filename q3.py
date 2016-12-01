from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "abcd"))
session = driver.session()

result = session.run("MATCH (c:Customer)-[cus:cus_ord]->(o:Order)-[item:Lineitem]->(e:Partsupp) WHERE c.c_mktsegment = 'Industry' AND o.o_order_date < 20161120 AND item.l_shipdate > 20161111 RETURN o.o_key, SUM(item.l_extended_price*(1-item.l_discount)) AS revenue, o.o_order_date, o.o_ship_priority ORDER BY revenue DESC, o.o_order_date")

for record in result:
    print("%s" % (record))
    print("")