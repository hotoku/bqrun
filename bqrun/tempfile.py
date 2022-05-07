from __future__ import annotations

import os


class TempFile:
    def __init__(self, fpath: str) -> None:
        self.fpath = fpath

    def __enter__(self) -> str:
        return self.fpath

    def __exit__(self, *_):
        if os.path.exists(self.fpath):
            os.remove(self.fpath)
