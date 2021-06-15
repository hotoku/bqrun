import unittest
import tempfile

from bqrun import bqrun

from .util import dump, WorkingDirectory


class TestDependency(unittest.TestCase):
    def test_dep1(self):
        """
        1ファイルしかないシンプルな場合
"""
        sql1 = """
create or replace table `p.d.t1` as 
select
  *
from
  `p.d.t2`
"""
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            deps = bqrun.parse_files(".", False)
        dep1 = deps[0]
        self.assertEqual(len(dep1.targets), 1)
        self.assertEqual(len(dep1.sources), 1)
        self.assertEqual(dep1.targets[0], "p.d.t1")
        self.assertEqual(dep1.sources[0], "p.d.t2")
        self.assertEqual(dep1.file, "1.sql")

    def test_dep2(self):
        """
        2ファイルが連続して実行される場合
"""
        sql1 = """
create or replace table `p.d.t1` as 
select
  *
from
  `p.d.t0`
"""
        sql2 = """
create or replace table `p.d.t2` as
select
  *
from
  `p.d.t1`
"""
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            dump(sql2, d, "2")
            deps = bqrun.parse_files(".", False)
        self.assertEqual(len(deps), 2)
        dep1_ = [
            d for d in deps if d.file == "1.sql"
        ]
        self.assertEqual(len(dep1_), 1)
        dep1 = dep1_[0]
        self.assertEqual(len(dep1.targets), 1)
        self.assertEqual(len(dep1.sources), 1)
        self.assertEqual(dep1.targets[0], "p.d.t1")
        self.assertEqual(dep1.sources[0], "p.d.t0")

        dep2_ = [
            d for d in deps if d.file == "2.sql"
        ]
        self.assertEqual(len(dep2_), 1)
        dep2 = dep2_[0]
        self.assertEqual(len(dep2.targets), 1)
        self.assertEqual(len(dep2.sources), 1)
        self.assertEqual(dep2.targets[0], "p.d.t2")
        self.assertEqual(dep2.sources[0], "p.d.t1")

    def test_dep3(self):
        """
        複数ソースがある場合
"""
        sql1 = """
create or replace table `p.d.t1` as 
select
  *
from
  `p.d.t101` left join `p.d.t102` using(id)
"""
        with tempfile.TemporaryDirectory() as d, WorkingDirectory(d):
            dump(sql1, d, "1")
            deps = bqrun.parse_files(".", False)
        self.assertEqual(len(deps), 1)
        dep1 = deps[0]
        self.assertEqual(len(dep1.targets), 1)
        self.assertEqual(len(dep1.sources), 2)
        self.assertEqual(dep1.targets[0], "p.d.t1")
        self.assertEqual(
            set(dep1.sources),
            set(["p.d.t101", "p.d.t102"]))


if __name__ == '__main__':
    unittest.main()
