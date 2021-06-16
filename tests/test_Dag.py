import unittest
from bqrun import bqrun
from io import StringIO
import tempfile


from .util import dump, WorkingDirectory


def remove_blank(m):
    lines = [
        l for l in
        map(lambda l: l.strip(), m.split("\n"))
        if len(l) > 0
    ]
    return lines


class TestDag(unittest.TestCase):
    def check3(self, v1, v2, expected):
        self.assertEqual(v1, expected)
        self.assertEqual(v2, expected)

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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            dump(sql2, d, "2")
            deps = bqrun.parse_files(".", False)

        dag = bqrun.Dag(deps)
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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            deps = bqrun.parse_files(".", False)

        dag = bqrun.Dag(deps)
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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            deps = bqrun.parse_files(".", False)

        dag = bqrun.Dag(deps)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: done.1

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -f done.*
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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            dump(sql2, d, "2")
            deps = bqrun.parse_files(".", False)
        dag = bqrun.Dag(deps)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: done.1 done.2

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@

done.2: 2.sql done.1
\tcat 2.sql | bq query
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -f done.*
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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            deps = bqrun.parse_files(".", False)
            deps2 = bqrun.parse_files(".", True)
        dag = bqrun.Dag(deps)
        dag2 = bqrun.Dag(deps2)
        self.check3(set(dag.targets),
                    set(dag2.targets),
                    {"done.1"})
        self.check3(set(dag.targets["done.1"]),
                    set(dag2.targets["done.1"]),
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
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            dump(sql2, d, "2")
            deps = bqrun.parse_files(".", False)
        dag = bqrun.Dag(deps)
        sio = StringIO()
        dag.create_makefile(sio)
        mf_act = sio.getvalue()
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: done.1 done.2

done.1: 1.sql
\tcat 1.sql | bq query
\ttouch $@

done.2: 2.sql done.1
\tcat 2.sql | bq query
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -f done.*
"""
        self.assertEqual(remove_blank(mf_act),
                         remove_blank(mf_exp))
        self.assertEqual(len(dag.orig_deps), 2)
        self.assertEqual(dag.orig_deps[0].sources,
                         ["p.d.t3"])


if __name__ == '__main__':
    unittest.main()
