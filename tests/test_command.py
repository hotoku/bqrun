import unittest
from bqrun import bqrun
from io import StringIO
import tempfile
import os


class TestCommand(unittest.TestCase):
    def test_docker(self):
        """
        Docker利用時のコマンド
        """
        ret = bqrun.setup_alphadag_command_docker(".", "/hoge/dag.dot")
        volume_output = "/hoge:/bqrun_output"
        volume_sql = f"{os.path.join(os.getcwd(), '.')}:/home"
        self.assertEqual(
            ret,
            [
                "docker",
                "run",
                "--rm",
                "-v", volume_output,
                "-v", volume_sql,
                "matts966/alphasql:latest",
                "alphadag",
                "--with_tables",
                "--output_path", "/bqrun_output/dag.dot",
                "."
            ]
        )

    def test_binary(self):
        """
        バイナリ利用時のコマンド
        """
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
 
