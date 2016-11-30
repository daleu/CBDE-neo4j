from neo4j.v1 import GraphDatabase, basic_auth
import datetime

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "abcd"))
session = driver.session()

#DATABASE INIT

date1 = datetime.datetime(2016, 11, 11)
date2 = datetime.datetime(2016, 11, 12)
integerdat1 = str(10000*date1.year+100*date1.month+date1.day)
integerdat2 = str(10000*date2.year+100*date2.month+date2.day)

#REGION
session.run("CREATE (reg1:Region {r_name:'Europe'})")
session.run("CREATE (reg2:Region {r_name:'America'})")

session.run("CREATE INDEX on:Region(r_name)")

#NATION
session.run("CREATE (nat1:Nation {n_name:'Spain'})")
session.run("CREATE (nat2:Nation {n_name:'France'})")
session.run("CREATE (nat3:Nation {n_name:'Colorado'})")
session.run("CREATE (nat4:Nation {n_name:'Pennsylvania'})")

session.run("CREATE INDEX on:Nation(n_name)")

#SUPPLIERS
session.run("CREATE (sup1:Supplier {s_suppkey:'key1', s_name:'Andreu', s_acctbal:4.91, s_address:'Barcelona, C/Sants', s_phone:'97381948', s_comment:'comentari1'})")
session.run("CREATE (sup2:Supplier {s_suppkey:'key2', s_name:'Marcel', s_acctbal:7.9, s_address:'Denver, C/Sants', s_phone:'1234', s_comment:'comentari2'})")
session.run("CREATE (sup3:Supplier {s_suppkey:'key3', s_name:'Gerard', s_acctbal:1.5, s_address:'Harrisburg, C/Sants', s_phone:'4321', s_comment:'comentari3'})")

#PARTNERS
session.run("CREATE (par1:Partner {p_partkey:'key1', p_mfgr:'mfgr1', p_type:'type1', p_size:4})")
session.run("CREATE (par2:Partner {p_partkey:'key2', p_mfgr:'mfgr2', p_type:'type2', p_size:2})")
session.run("CREATE (par3:Partner {p_partkey:'key3', p_mfgr:'mfgr3', p_type:'type2', p_size:4})")
session.run("CREATE (par4:Partner {p_partkey:'key4', p_mfgr:'mfgr4', p_type:'type1', p_size:2})")

session.run("CREATE INDEX on:Partner(p_type)")

#PARTSUPPS
session.run("CREATE (parsup1:Partsupp {ps_partkey:'key1', ps_suppkey:'key1', ps_supplycost:10})")
session.run("CREATE (parsup2:Partsupp {ps_partkey:'key1', ps_suppkey:'key2', ps_supplycost:1})")
session.run("CREATE (parsup3:Partsupp {ps_partkey:'key1', ps_suppkey:'key3', ps_supplycost:4})")
session.run("CREATE (parsup4:Partsupp {ps_partkey:'key2', ps_suppkey:'key1', ps_supplycost:13})")
session.run("CREATE (parsup5:Partsupp {ps_partkey:'key4', ps_suppkey:'key2', ps_supplycost:2})")
session.run("CREATE (parsup6:Partsupp {ps_partkey:'key3', ps_suppkey:'key1', ps_supplycost:7})")

session.run("CREATE INDEX on:Partsupp(ps_supplycost)")

#CUSTOMERS
session.run("CREATE (cus1:Customer {c_name:'Miquel', c_mktsegment:'Industry'})")
session.run("CREATE (cus2:Customer {c_name:'Albert', c_mktsegment:'Agriculture'})")
session.run("CREATE (cus3:Customer {c_name:'David', c_mktsegment:'Industry'})")
session.run("CREATE (cus4:Customer {c_name:'Jordi', c_mktsegment:'Agriculture'})")
session.run("CREATE (cus5:Customer {c_name:'Dani', c_mktsegment:'Industry'})")

session.run("CREATE INDEX on:Customer(c_mktsegment)")

#ORDERS
session.run("CREATE (ord1:Order {o_key:'key1', o_order_date:20161111, o_ship_priority:2})")
session.run("CREATE (ord2:Order {o_key:'key2', o_order_date:20161112, o_ship_priority:1})")
session.run("CREATE (ord3:Order {o_key:'key3', o_order_date:20161111, o_ship_priority:3})")
session.run("CREATE (ord4:Order {o_key:'key4', o_order_date:20161112, o_ship_priority:1})")
session.run("CREATE (ord5:Order {o_key:'key5', o_order_date:20161111, o_ship_priority:2})")
session.run("CREATE (ord6:Order {o_key:'key6', o_order_date:20161112, o_ship_priority:1})")
session.run("CREATE (ord7:Order {o_key:'key7', o_order_date:20161111, o_ship_priority:3})")

session.run("CREATE INDEX on:Order(o_order_date)")

############################RELATIONS########################################

#LINEITEMS (ORDER_PARTSUPP)
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key1' AND (b.ps_partkey='key1' AND b.ps_suppkey='key1') CREATE (a)-[r:Lineitem {l_returnflag:'1', l_linestatus:'shipped', l_quantity:1, l_extended_price:20, l_discount:0.2, l_tax:0.21,l_shipdate:20161111}]->(b)")
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key1' AND (b.ps_partkey='key1' AND b.ps_suppkey='key2') CREATE (a)-[r:Lineitem {l_returnflag:'0', l_linestatus:'shipped', l_quantity:3, l_extended_price:10, l_discount:0, l_tax:0.8,l_shipdate:20161111}]->(b)")

session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key2' AND (b.ps_partkey='key1' AND b.ps_suppkey='key1') CREATE (a)-[r:Lineitem {l_returnflag:'1', l_linestatus:'shipped', l_quantity:4, l_extended_price:4, l_discount:0.3, l_tax:0.11,l_shipdate:20161112}]->(b)")
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key2' AND (b.ps_partkey='key2' AND b.ps_suppkey='key1') CREATE (a)-[r:Lineitem {l_returnflag:'1', l_linestatus:'ordered', l_quantity:2, l_extended_price:10, l_discount:0.1, l_tax:0.11,l_shipdate:20161112}]->(b)")
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key2' AND (b.ps_partkey='key4' AND b.ps_suppkey='key2') CREATE (a)-[r:Lineitem {l_returnflag:'0', l_linestatus:'shipped', l_quantity:1, l_extended_price:40, l_discount:0.1, l_tax:0.21,l_shipdate:20161112}]->(b)")

session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key3' AND (b.ps_partkey='key1' AND b.ps_suppkey='key1') CREATE (a)-[r:Lineitem {l_returnflag:'0', l_linestatus:'ordered', l_quantity:6, l_extended_price:2, l_discount:0.3, l_tax:0.11,l_shipdate:20161111}]->(b)")
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key3' AND (b.ps_partkey='key3' AND b.ps_suppkey='key1') CREATE (a)-[r:Lineitem {l_returnflag:'1', l_linestatus:'shipped', l_quantity:1, l_extended_price:10, l_discount:0.1, l_tax:0.11,l_shipdate:20161111}]->(b)")
session.run("MATCH (a:Order),(b:Partsupp) WHERE a.o_key='key3' AND (b.ps_partkey='key4' AND b.ps_suppkey='key2') CREATE (a)-[r:Lineitem {l_returnflag:'0', l_linestatus:'shipped', l_quantity:1, l_extended_price:3, l_discount:0, l_tax:0.21,l_shipdate:20161111}]->(b)")

#REG_NATION
session.run("MATCH (a:Region),(b:Nation) WHERE a.r_name='Europe' AND (b.n_name='Spain' OR b.n_name='France') CREATE (a)-[r:reg_nat]->(b)")
session.run("MATCH (a:Region),(b:Nation) WHERE a.r_name='America' AND (b.n_name='Pennsylvania' OR b.n_name='Colorado') CREATE (a)-[r:reg_nat]->(b)")

#REG_SUPPLIER
session.run("MATCH (a:Region),(b:Supplier) WHERE a.r_name='America' AND (b.s_name='Gerard' OR b.s_name='Marcel') CREATE (a)-[r:reg_supp]->(b)")
session.run("MATCH (a:Region),(b:Supplier) WHERE a.r_name='Europe' AND b.s_name='Andreu' CREATE (a)-[r:reg_supp]->(b)")

#SUPPLIER_NATION
session.run("MATCH (a:Supplier),(b:Nation) WHERE a.s_name='Gerard' AND b.n_name='Pennsylvania' CREATE (a)-[r:supp_nat]->(b)")
session.run("MATCH (a:Supplier),(b:Nation) WHERE a.s_name='Andreu' AND b.n_name='Spain' CREATE (a)-[r:supp_nat]->(b)")
session.run("MATCH (a:Supplier),(b:Nation) WHERE a.s_name='Marcel' AND b.n_name='Colorado' CREATE (a)-[r:supp_nat]->(b)")

#NATION_CUSTOMER
session.run("MATCH (a:Nation),(b:Customer) WHERE a.n_name='Spain' AND (b.c_name='Miquel' OR b.c_name='Albert' OR b.c_name='David') CREATE (a)-[r:nat_cus]->(b)")
session.run("MATCH (a:Nation),(b:Customer) WHERE a.n_name='France' AND (b.c_name='Jordi' OR b.c_name='Dani') CREATE (a)-[r:nat_cus]->(b)")

#CUSTOMER_ORDER
session.run("MATCH (a:Customer),(b:Order) WHERE a.c_name='Miquel' AND (b.o_key='key1' OR b.o_key='key2') CREATE (a)-[r:cus_ord]->(b)")
session.run("MATCH (a:Customer),(b:Order) WHERE a.c_name='Albert' AND b.o_key='key3' CREATE (a)-[r:cus_ord]->(b)")
session.run("MATCH (a:Customer),(b:Order) WHERE a.c_name='David' AND b.o_key='key4' CREATE (a)-[r:cus_ord]->(b)")
session.run("MATCH (a:Customer),(b:Order) WHERE a.c_name='Jordi' AND b.o_key='key5' CREATE (a)-[r:cus_ord]->(b)")
session.run("MATCH (a:Customer),(b:Order) WHERE a.c_name='Dani' AND (b.o_key='key6' OR b.o_key='key7') CREATE (a)-[r:cus_ord]->(b)")

#SUPPLIER_PARTSUPP
session.run("MATCH (a:Supplier),(b:Partsupp) WHERE a.s_suppkey=b.ps_suppkey CREATE (a)-[r:sup_parsup]->(b)")

#PARTSUPP_PARTNER
session.run("MATCH (a:Partsupp),(b:Partner) WHERE a.ps_partkey=b.p_partkey CREATE (a)-[r:parsup_par]->(b)")

result = session.run("MATCH (a:Person) WHERE a.name = 'Arthur' RETURN a.name AS name, a.title AS title")
for record in result:
    print("%s %s" % (record["title"], record["name"]))

session.close()
