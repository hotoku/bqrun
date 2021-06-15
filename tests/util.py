import os


def dump(s, d, f):
    fpath = os.path.join(d, f) + ".sql"
    with open(fpath, "w") as fp:
        fp.write(s)

class WorkingDirectory:
    def __init__(self, target_dir):
        self.curdir_backup = None
        self.target_dir = target_dir

    def __enter__(self):
        self.curdir_backup = os.getcwd()
        os.chdir(self.target_dir)

    def __exit__(self, *args):
        os.chdir(self.curdir_backup)
