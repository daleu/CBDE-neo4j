from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "abcd"))
session = driver.session()

result1 = session.run("MATCH (a:Region)-[b:reg_supp]-(c:Supplier)-[d:sup_parsup]->(e:Partsupp) WHERE a.r_name = 'America' RETURN MIN(e.ps_supplycost) AS min")

result = ""

for record in result1:
    result = session.run("MATCH ((a:Region)-[b:reg_supp]-(c:Supplier)-[d:sup_parsup]-(e:Partsupp)-[f:parsup_par]->(g:Partner)) MATCH ((x:Supplier)-[y:supp_nat]->(z:Nation)) WHERE x.s_name = c.s_name AND a.r_name = 'America' AND "+str(record['min'])+"=e.ps_supplycost AND g.p_type='type1' AND g.p_size=4 RETURN c.s_acctbal, c.s_name, z.n_name, e.p_partkey, e.p_mfgr, c.s_address, c.s_phone, c.s_comment")

for record in result:
    print("%s" % (record))
    print("")

session.close()
