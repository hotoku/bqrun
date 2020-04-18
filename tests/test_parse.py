import unittest
from bqrun import parse


class TestParse(unittest.TestCase):
    def test_parse(self):
        """
        simple select
        """
        sql = """
select
  *
from
  `p.d.t`
"""
        t, s = parse.parse(sql)
        self.assertEqual(t, [])
        self.assertEqual(s, ["p.d.t"])

    def test_parse2(self):
        """
        select from join
        """
        sql = """
select
  *
from
  `p.d.t1` left join `p.d.t2`
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, [])
        self.assertEqual(set(s), set(["p.d.t1", "p.d.t2"]))

    def test_parse3(self):
        """
        create or replace with join

        """
        sql = """
create or replace table `p.d.t3` as
select
  *
from
  `p.d.t1` left join `p.d.t2`
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, ["p.d.t3"])
        self.assertEqual(set(s), set(["p.d.t1", "p.d.t2"]))

    def test_parse4(self):
        """
        multiple sentences
        """
        sql = """
create or replace table `p.d.t3` as
select
  *
from
  `p.d.t1` left join `p.d.t2`;

create or replace table `p.d.t4` as
select
  *
from
  `p.d.t1` left join `p.d.t2`;
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, ["p.d.t3", "p.d.t4"])
        self.assertEqual(set(s), set(["p.d.t2", "p.d.t1"]))

    def test_parse5(self):
        """
        create sentence
        """

        sql = """
create table `p.d.t3` as
select
  *
from
  `p.d.t1` left join `p.d.t2`
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, ["p.d.t3"])
        self.assertEqual(set(s), set(["p.d.t2", "p.d.t1"]))

    def test_parse6(self):
        """
        nested query
        """

        sql = """
create table `p.d.t3` as
select
  *
from
  (select * from `p.d.t1`)
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, ["p.d.t3"])
        self.assertEqual(set(s), set(["p.d.t1"]))

    def test_parse7(self):
        """
        with clause
        """

        sql = """
create table `p.d.t3` as
with temp1 as 
(select * from select * from `p.d.t1`),
temp2 as 
(select * from select * from `p.d.t2`)
select * from temp1 left join temp2
"""

        t, s = parse.parse(sql)
        self.assertEqual(t, ["p.d.t3"])
        self.assertEqual(set(s), set(["p.d.t1", "p.d.t2"]))


if __name__ == '__main__':
    unittest.main()
