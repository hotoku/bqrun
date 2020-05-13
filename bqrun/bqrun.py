#!/usr/bin/env python


import functools as ft
import argparse
import sqlparse as sp
from sqlparse.tokens import Whitespace, Newline, Keyword, Name, DDL
from sqlparse.sql import TokenList, Comment
from glob import glob
import logging
import re
import subprocess
import jinja2


def flatten(lss):
    return ft.reduce(lambda x, y: x+y, list(lss))


def is_neligible(token):
    return (token.ttype is Whitespace) or \
        (token.ttype is Newline) or \
        (token.ttype is None and isinstance(token, Comment)) or \
        (token.ttype is Name and token.value == "#standardSQL")


def extract(tokens):
    tokens = [t for t in tokens if not is_neligible(t)]
    for t in tokens:
        if isinstance(t, TokenList):
            yield from extract(t)
        else:
            yield t


class ParseError(Exception):
    pass


class UnexpectedToken(ParseError):
    def __init__(self, exp, got, tokens):
        super(UnexpectedToken, self).__init__(
            f"expected {exp} but got {got}:\n{' '.join([t.value for t in tokens])}")


def term(val, ttype):
    def ret(tokens):
        token = tokens[0]
        if token.ttype is ttype and token.value.upper() == val.upper():
            return 1
        else:
            raise UnexpectedToken(val.upper(), token.value, tokens)
    return ret


create_term = term("CREATE", DDL)
table_term = term("TABLE", Keyword)
as_term = term("AS", Keyword)


def keyword(val):
    def ret(token):
        return (token.ttype is Keyword and
                token.value.upper() == val.upper())
    return ret


as_keyword = keyword("as")
function_keyword = keyword("function")


def create_or_replace_term(tokens):
    token = tokens[0]
    if token.ttype is DDL and \
       re.match(r"CREATE\s+OR\s+REPLACE", token.value.upper()):
        return 1
    else:
        raise UnexpectedToken("CREATE OR REPLACE", token.value, tokens)


def table_name(tokens, ls):
    token = tokens[0]
    if token.ttype is Name:
        ls.append(token.value.replace("`", ""))
        return 1
    else:
        raise UnexpectedToken("TABLE NAME", token.value, tokens)


def create_sentence(tokens, targets, sources):
    pos = 0
    if tokens[pos].value.upper() == "CREATE":
        pos += create_term(tokens)
    else:
        pos += create_or_replace_term(tokens)
    pos += table_term(tokens[pos:])
    pos += table_name(tokens[pos:], targets)
    while not as_keyword(tokens[pos]):
        pos += 1
    pos += as_term(tokens[pos:])
    gather_sources(tokens[pos:], sources)
    return targets, sources


def gather_sources(tokens, sources):
    for t in tokens:
        m = re.match("`(.+)`", t.value)
        if m:
            sources.append(m[1])


def analyze_statement(st):
    tokens = [t for t in extract(st)]
    sources, targets = [], []
    if len(tokens) == 0:
        return sources, targets
    if re.match("CREATE.*", tokens[0].value.upper()):
        create_sentence(tokens, targets, sources)
    else:
        gather_sources(tokens, sources)
    return targets, sources


def parse(sql):
    targets = []
    sources = []
    statements = sp.parse(sql)
    for s in statements:
        t, s = analyze_statement(s)
        targets += t
        sources += s
    sources2 = list(set(sources))
    return targets, sources2


def done(f):
    return "done." + re.sub(r"(.*).sql$", r"\1", f)


class Dependency:
    def __init__(self, t, s, f):
        self.targets = t
        self.sources = s
        self.file = f

    def filter(self, targets):
        s = [s for s in self.sources if s in targets]
        return Dependency(self.targets, s, self.file)


class Dag:
    @staticmethod
    def done(f):
        return "done." + re.sub(r"(.*).sql$", r"\1", f)

    @staticmethod
    def undone(t):
        return re.sub(r"^done.", "", t) + ".sql"

    def __init__(self, ds):
        targets = flatten([d.targets for d in ds])
        if len(targets) != len(set(targets)):
            raise RuntimeError(
                f"some targets are defined in multiple files: {targets}")
        ds2 = [d.filter(targets) for d in ds]
        sources = flatten([d.sources for d in ds2])

        def search(s):
            for d in ds2:
                if s in d.targets:
                    return d.file
            raise RuntimeError(
                f"source {s} is not defined anywhere")

        s2f = {
            s: search(s)
            for s in sources
        }

        self.targets = {}
        for d in ds2:
            key = Dag.done(d.file)
            val = {d.file}
            for s in d.sources:
                f = s2f[s]
                if f == d.file:
                    continue
                val.add(Dag.done(f))
            self.targets[key] = val

    def rule(self, t):
        ret = """
{t}: {ss}
\tcat {f} | bq query
\ttouch $@
""".strip().format(
            t=t,
            ss=" ".join(sorted(self.targets[t])),
            f=Dag.undone(t)
        )
        return ret

    def create_makefile(self, sink):
        template = jinja2.Template("""
.PHONY: all
all: {{ targets }}

{% for r in rules %}
{{ r }}
{% endfor %}
""".strip())
        lines = template.render(dict(
            targets=" ".join(sorted(self.targets)),
            rules=[
                self.rule(t)
                for t in self.targets
            ]
        ))
        sink.write(lines)


def main(args):
    fnames = glob("*.sql")
    dependencies = []
    for fname in fnames:
        with open(fname) as f:
            sql = "\n".join(f.readlines())
        t, s = parse(sql)
        dependencies.append(Dependency(t, s, fname))

    dag = Dag(dependencies)
    with open("Makefile", "w") as f:
        dag.create_makefile(f)

    cmd = ["make", "-j", str(args.parallel)]
    if args.dry_run:
        cmd.append("-n")
    subprocess.run(cmd)


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parallel", default=8)
    parser.add_argument("-n", "--dry-run", default=False,
                        action="store_true")
    return parser


def setup_logging():
    logging.basicConfig(
        filename="parse.log",
        level=logging.DEBUG,
        format="[%(levelname)s]%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
