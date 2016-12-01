from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "abcd"))
session = driver.session()

result = session.run("MATCH ((r:Region)-[rn:reg_nat]->(n:Nation)-[nc:nat_cust]->(c:Customer)-[cus:cus_ord]->(o:Order)-[item:Lineitem]->(ps:Partsupp)) MATCH ((rsn:Region)-[rspssn:reg_supp]->(s:Supplier)-[sn:supp_nat]->(ns:Nation)) MATCH((rsps:Region)-[rspssl:reg_supp]->(sl:Supplier)-[sps:sup_parsup]->(pssup:Partsupp)) WHERE n.n_name = ns.n_name AND r.r_name = 'Europe' AND rsn.r_name = 'Europe' AND rsps.r_name = 'Europe' AND o.o_order_date >= 20161111 AND o.o_order_date < 20171112 AND ps.ps_suppkey = pssup.ps_suppkey AND s.s_suppkey = sl.s_suppkey RETURN n.n_name, SUM(item.l_extended_price*(1-item.l_discount)) AS revenue ORDER BY revenue desc ")

for record in result:
    print("%s" % (record))
    print("")