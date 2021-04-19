#!/usr/bin/env python


import sys
import functools as ft
import argparse
from glob import glob
import logging
import re
import subprocess
import itertools as it
import tempfile

import jinja2
from networkx.drawing.nx_pydot import read_dot
import os


def flatten(lss):
    if len(lss) == 0:
        return []
    return ft.reduce(lambda x, y: x + y, list(lss))


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


class NotDefinedTableException(Exception):
    pass


class MultipleDefinitionExcetion(Exception):
    pass


def defined_file(t, deps):
    ret = []
    for d in deps:
        if t in d.targets:
            ret.append(d.file)
    if len(ret) == 0:
        raise NotDefinedTableException(
            f"source {t} is not defined anywhere")
    if len(ret) > 1:
        raise MultipleDefinitionExcetion(
            f"source {t} is defined in multiple file: {ret}")
    return ret[0]


class Dag:
    @staticmethod
    def done(f):
        return "done." + re.sub(r"(.*).sql$", r"\1", f)

    @staticmethod
    def undone(t):
        return re.sub(r"^done.", "", t) + ".sql"

    def __init__(self, ds):
        self.deps = self.setup_dependencies(ds, True)
        self.orig_deps = self.setup_dependencies(ds, False)
        self.targets = self.setup_targets()

    def setup_dependencies(self, ds, flter):
        targets = flatten([d.targets for d in ds])
        if len(targets) != len(set(targets)):
            raise RuntimeError(
                f"some targets are defined in multiple files: {targets}")
        if flter:
            return [d.filter(targets) for d in ds]
        else:
            return ds

    def setup_targets(self):
        sources = flatten([d.sources for d in self.deps])

        s2f = {
            s: defined_file(s, self.deps)
            for s in sources
        }

        ret = {}
        for d in self.deps:
            key = Dag.done(d.file)
            val = {d.file}
            for s in d.sources:
                f = s2f[s]
                if f == d.file:
                    continue
                val.add(Dag.done(f))
            ret[key] = val
        return ret

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

.PHONY: clean
clean:
\trm -f done.*
""".lstrip())
        lines = template.render(dict(
            targets=" ".join(sorted(self.targets)),
            rules=[
                self.rule(t)
                for t in self.targets
            ]
        ))
        sink.write(lines)

    def create_dotfile(self, sink):
        template = jinja2.Template("""
digraph {
  graph [
         charset = "UTF-8",
         labelloc = "t",
         labeljust = "c",
         bgcolor = "#343434",
         fontcolor = white,
         fontsize = 18,
         style = "filled",
         rankdir = TB,
         margin = 0.2,
         splines = spline,
         ranksep = 1.0,
         nodesep = 0.9
         ];
  node [
        colorscheme = "rdylgn11",
        style = "solid,filled",
        fontsize = 16,
        fontcolor = 6,
        fontname = "Migu 1M",
        color = 7,
        fillcolor = 11,
        height = 0.6,
        width = 1.2
        ];

  edge [
        style = solid,
        fontsize = 14,
        fontcolor = white,
        fontname = "Migu 1M",
        color = white,
        labelfloat = true,
        labeldistance = 2.5,
        labelangle = 70
        ];

{% for e in edges %}
"{{e[0]}}" -> "{{e[1]}}";
{% endfor %}

{% for n in nodes %}
"{{n}}" [ shape = ellipse ];
{% endfor %}
}
""")

        def tblname(s):
            return s.split(".")[-1]

        def edgelabel(s):
            ret = tblname(s)
            try:
                f = defined_file(s, self.deps)
                return ret + f"[{f}]"
            except NotDefinedTableException:
                return ret

        def dep2edge(dep):
            return [
                (edgelabel(s), edgelabel(e)) for s, e in
                it.product(dep.sources, dep.targets)
            ]

        edges = flatten([
            dep2edge(d) for d in
            self.orig_deps
        ])
        nodes = [edgelabel(t) for d in self.deps for t in d.targets] + \
            [edgelabel(t) for d in self.deps for t in d.sources]

        lines = template.render(dict(
            edges=edges,
            nodes=nodes
        ))
        sink.write(lines)


def create_makefile(dag, makefile):
    with open(makefile, "w") as f:
        dag.create_makefile(f)


def run_query(parallel, dryrun, makefile):
    cmd = ["make", "-j", str(parallel), "-f", makefile]
    if dryrun:
        cmd.append("-n")
    subprocess.run(cmd)


def create_graph(dag):
    fname = "graph.dot"
    with open(fname, "w") as f:
        dag.create_dotfile(f)
    cmd = ["dot", "-Kdot", "-Tpng", fname,
           "-o" + re.sub(".dot$", ".png", fname)]
    subprocess.run(cmd)


def strip_quote(s):
    return s.strip('"`')


def make_dependency(graph, nodes, query_node, file_):
    sources = [graph.nodes[n] for n in graph.predecessors(query_node["id"])]
    targets = [graph.nodes[n] for n in graph.successors(query_node["id"])]
    s2 = [s["label"] for s in sources]
    t2 = [t["label"] for t in targets]
    s3 = [strip_quote(s) for s in s2]
    t3 = [strip_quote(t) for t in t2]
    return Dependency(t3, s3, os.path.split(strip_quote(file_))[-1])


def parse_files(target_dir):
    with tempfile.TemporaryDirectory() as d:
        fpath = os.path.join(d, "dag.dot")
        r = subprocess.run([
            "alphadag",
            "--with_tables",
            "--output_path", fpath,
            target_dir
        ])
        with open(fpath) as f:
            sys.stderr.write("=== dotfile ===")
            sys.stderr.write("".join(f.readlines()))

        g = read_dot(fpath)

    nodes = [dict(g.nodes[n], id=n) for n in g.nodes]
    queries = [x for x in nodes if x["type"] == "query"]
    tables = [x for x in nodes if x["type"] == "table"]

    return [
        make_dependency(g, nodes, q, q["label"])
        for q in queries
    ]


def print_ignore_lines():
    print("""done.*
parse.log
graph.png
graph.dot""")


def main(args):
    dependencies = parse_files(".")
    dag = Dag(dependencies)
    if args.ignore:
        print_ignore_lines()
        sys.exit(0)

    create_makefile(dag, args.makefile)
    create_graph(dag)
    run_query(args.parallel, args.dry_run, args.makefile)


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parallel", default=8)
    parser.add_argument("-n", "--dry-run", default=False,
                        action="store_true")
    parser.add_argument("--project", default=None)
    parser.add_argument("-i", "--ignore", default=False,
                        action="store_true", help="print lines for .gitignore")
    parser.add_argument("-m", "--makefile", default="Makefile")
    return parser


def setup_logging():
    logging.basicConfig(
        filename="parse.log",
        level=logging.DEBUG,
        format="[%(levelname)s]%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
