import unittest
from bqrun import bqrun
from io import StringIO
import tempfile


from .util import dump, WorkingDirectory


def remove_blank(m):
    lines = [
        line for line in
        map(lambda l: l.strip(), m.split("\n"))
        if len(line) > 0
    ]
    return lines


class TestDag(unittest.TestCase):
    def check3(self, v2, expected):
        self.assertEqual(v2, expected)

    @staticmethod
    def with_stringio(func):
        sio = StringIO()
        func(sio)
        return sio.getvalue()

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
            deps2 = bqrun.parse_files(".")

        dag2 = bqrun.Dag(deps2)
        self.check3(set(dag2.targets),
                    {".bqrun/done.1", ".bqrun/done.2"})
        self.check3(set(dag2.targets[".bqrun/done.1"]),
                    {"1.sql"})
        self.check3(set(dag2.targets[".bqrun/done.2"]),
                    {"2.sql", ".bqrun/done.1"})

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
            deps2 = bqrun.parse_files(".")

        dag2 = bqrun.Dag(deps2)
        self.check3(set(dag2.targets),
                    {".bqrun/done.1"})
        self.check3(set(dag2.targets[".bqrun/done.1"]),
                    {"1.sql"})

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
            deps2 = bqrun.parse_files(".")

        dag2 = bqrun.Dag(deps2)
        mf_act2 = self.with_stringio(dag2.create_makefile)
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: .bqrun/done.1

.bqrun/done.1: 1.sql
\tcat 1.sql | bq query --nouse_legacy_sql
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -rf .bqrun
""".strip()
        self.check3(remove_blank(mf_act2),
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
            deps2 = bqrun.parse_files(".")
        dag2 = bqrun.Dag(deps2)
        mf_act2 = self.with_stringio(dag2.create_makefile)
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: .bqrun/done.1 .bqrun/done.2

.bqrun/done.1: 1.sql
\tcat 1.sql | bq query --nouse_legacy_sql
\ttouch $@

.bqrun/done.2: .bqrun/done.1 2.sql
\tcat 2.sql | bq query --nouse_legacy_sql
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -rf .bqrun
"""
        self.check3(remove_blank(mf_act2),
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
            deps2 = bqrun.parse_files(".")
        dag2 = bqrun.Dag(deps2)
        self.check3(set(dag2.targets),
                    {".bqrun/done.1"})
        self.check3(set(dag2.targets[".bqrun/done.1"]),
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
            deps2 = bqrun.parse_files(".")
        dag2 = bqrun.Dag(deps2)
        mf_act2 = self.with_stringio(dag2.create_makefile)
        mf_exp = """
.PHONY: bqrun-all
bqrun-all: .bqrun/done.1 .bqrun/done.2

.bqrun/done.1: 1.sql
\tcat 1.sql | bq query --nouse_legacy_sql
\ttouch $@

.bqrun/done.2: .bqrun/done.1 2.sql
\tcat 2.sql | bq query --nouse_legacy_sql
\ttouch $@

.PHONY: bqrun-clean
bqrun-clean:
\trm -rf .bqrun
"""
        self.check3(remove_blank(mf_act2),
                    remove_blank(mf_exp))
        self.check3(len(dag2.orig_deps), 2)
        self.check3(dag2.orig_deps[0].sources,
                    ["p.d.t3"])


if __name__ == '__main__':
    unittest.main()
