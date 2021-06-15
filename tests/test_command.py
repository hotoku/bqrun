import unittest
from bqrun import bqrun
from io import StringIO
import tempfile


class TestCommand(unittest.TestCase):
    def test_docker(self):
        ret = bqrun.setup_alphadag_command_docker(".", "/hoge/dag.dot")
        self.assertEqual(
            ret,
            [
                "docker",
                "run",
                "--rm",
                "-v", "/hoge:/home",
                "matts966/alphasql:latest",
                "alphadag",
                "--with_tables",
                "--output_path", "./dag.dot",
                "."
            ]
        )

    def test_binary(self):
        ret = bqrun.setup_alphadag_command_binary(".", "/hoge/dag.dot")
        self.assertEqual(
            ret,
            [
                "alphadag",
                "--with_tables",
                "--output_path", "/hoge/dag.dot",
                "."
            ]
        )
 
