import unittest
from bqrun import bqrun
from io import StringIO


def remove_blank(m):
    lines = [
        l for l in
        map(lambda l: l.strip(), m.split("\n"))
        if len(l) > 0
    ]
    return lines


class TestDag(unittest.TestCase):
    def test_dag1(self):
        """
        build dag of 2 targets
        """

        sql1 = """
    create or replace table `p.d.t1` as
    select * from unnest([1,2,3])
    """
        sql2 = """
    create or replace table `p.d.t2` as
    select * from `p.d.t1`
    """
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql"),
            bqrun.Dependency(*bqrun.parse(sql2), "2.sql")
        ]
        dag = bqrun.Dag(ds)
        self.assertEqual(set(dag.targets),
                         set(["done.1", "done.2"]))
        self.assertEqual(set(dag.targets["done.1"]),
                         set(["1.sql"]))
        self.assertEqual(set(dag.targets["done.2"]),
                         set(["2.sql", "done.1"]))

    def test_dag2(self):
        """
        select table which created in the same file
        """
        sql1 = """
create or replace table `p.d.t1` as
select * from unnest([1,2,3]);

create or replace table `p.d.t2` as
select * from `p.d.t1`
"""
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql")
        ]
        dag = bqrun.Dag(ds)
        self.assertEqual(set(dag.targets),
                         set(["done.1"]))
        self.assertEqual(set(dag.targets["done.1"]),
                         set(["1.sql"]))

    def test_dag3(self):
        """
        makefile with 1 input
        """
        sql1 = """
create or replace table `p.d.t1` as
select * from unnest([1,2,3])
"""
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql")
        ]
        dag = bqrun.Dag(ds)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: all
all: done.1

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@
""".strip()

        self.assertEqual(remove_blank(mf_act),
                         remove_blank(mf_exp))

    def test_dag4(self):
        """
        makefile with 2 input
        """
        sql1 = """
create or replace table `p.d.t1` as
select * from unnest([1,2,3])
"""
        sql2 = """
create or replace table `p.d.t2` as
select * from `p.d.t1`
"""
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql"),
            bqrun.Dependency(*bqrun.parse(sql2), "2.sql")
        ]
        dag = bqrun.Dag(ds)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: all
all: done.1 done.2

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@

done.2: 2.sql done.1
\tcat 2.sql | bq query
\ttouch $@
"""
        self.assertEqual(remove_blank(mf_act),
                         remove_blank(mf_exp))

    def test_dag5(self):
        """
        read tables that is not created by other sql
        """
        sql1 = """
create or replace table `p.d.t1` as
select * from `x`
"""
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql")
        ]
        dag = bqrun.Dag(ds)
        self.assertEqual(set(dag.targets),
                         set(["done.1"]))
        self.assertEqual(set(dag.targets["done.1"]),
                         {"1.sql"})

    def test_dag6(self):
        """
        makefile with 2 input with external table
        """
        sql1 = """
create or replace table `p.d.t1` as
select * from `p.d.t3`
"""
        sql2 = """
create or replace table `p.d.t2` as
select * from `p.d.t1`
"""
        ds = [
            bqrun.Dependency(*bqrun.parse(sql1), "1.sql"),
            bqrun.Dependency(*bqrun.parse(sql2), "2.sql")
        ]
        dag = bqrun.Dag(ds)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: all
all: done.1 done.2

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@

done.2: 2.sql done.1
\tcat 2.sql | bq query
\ttouch $@
"""
        self.assertEqual(remove_blank(mf_act),
                         remove_blank(mf_exp))
        self.assertEqual(len(dag.orig_deps), 2)
        self.assertEqual(dag.orig_deps[0].sources,
                         ["p.d.t3"])


if __name__ == '__main__':
    unittest.main()
