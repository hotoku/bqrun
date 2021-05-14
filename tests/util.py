import os


def dump(s, d, f):
    fpath = os.path.join(d, f) + ".sql"
    with open(fpath, "w") as fp:
        fp.write(s)
