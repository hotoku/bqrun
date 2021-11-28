#!/usr/bin/env python


import sys
import functools as ft
import argparse
import logging
import re
import subprocess
import itertools as it
import tempfile
import io

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
\tcat {f} | bq query --nouse_legacy_sql
\ttouch $@
""".strip().format(
            t=t,
            ss=" ".join(sorted(self.targets[t])),
            f=Dag.undone(t)
        )
        return ret

    def create_makefile(self, sink):
        template = jinja2.Template("""
.PHONY: bqrun-all
bqrun-all: {{ targets }}

{% for r in rules %}
{{ r }}
{% endfor %}

.PHONY: bqrun-clean
bqrun-clean:
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
    sio = io.StringIO()
    dag.create_makefile(sio)
    if os.path.exists(makefile):
        with open(makefile, "r") as f:
            lines = [l.rstrip() for l in f.readlines()]
    else:
        lines = []
    mark = "# === bqrun: 44d98c928b0ecb5795e5182edf8329c828cb3968 ==="
    ret = []
    for line in lines:
        if line == mark:
            break
        ret.append(line)
    ret.append(mark + "\n"*2)

    ret += sio.getvalue().split("\n")
    with open(makefile, "w") as f:
        f.writelines("\n".join(ret))


def run_query(parallel, dryrun, makefile):
    cmd = ["make", "-j", str(parallel), "-f", makefile, "bqrun-all"]
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


class ParseError(Exception):
    pass


def setup_alphadag_command_docker(target_dir, output_path):
    temp_dir, f = os.path.split(output_path)
    target_dir_abs = os.path.join(os.getcwd(), target_dir)
    volume_dot = f"{temp_dir}:/bqrun_output"
    volume_sql = f"{target_dir_abs}:/home"
    ret = [
        "docker",
        "run",
        "--rm",
        "-v", volume_dot,
        "-v", volume_sql,
        "matts966/alphasql:latest",
        "alphadag",
        "--with_tables",
        "--output_path", f"/bqrun_output/{f}",
        target_dir
    ]
    return ret


def setup_alphadag_command_binary(target_dir, output_path):
    ret = [
        "alphadag",
        "--with_tables",
        "--output_path", output_path,
        target_dir
    ]
    return ret


def parse_files(target_dir, use_docker):
    if os.path.isabs(target_dir):
        raise ValueError("target_dir should be relative path.")
    with tempfile.TemporaryDirectory() as d:
        fpath = os.path.join(d, "dag.dot")
        args = [target_dir, fpath]
        cmd = setup_alphadag_command_docker(
            *args) if use_docker else setup_alphadag_command_binary(*args)
        ret = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        if ret.returncode != 0:
            raise ParseError(f"""message from alphadag:
stdout: {ret.stdout.decode("utf-8")}
stderr: {ret.stderr.decode("utf-8")}""")
        with open(fpath) as f:
            sys.stderr.write("=== dotfile ===\n")
            sys.stderr.write("".join(f.readlines()))

        g = read_dot(fpath)

    nodes = [dict(v, id=k) for k, v in g.nodes.items() if "type" in v]
    queries = [x for x in nodes if x["type"] == "query"]

    return [
        make_dependency(g, nodes, q, q["label"])
        for q in queries
    ]


def print_ignore_lines():
    print("""done.*
parse.log
graph.png
graph.dot""")


def clean(makefile):
    subprocess.run([
        "make",
        "-f",
        makefile,
        "bqrun-clean"  # todo: DRY this target name
    ])


def print_version():
    from . import __version__
    print("bqrun: version =", __version__)


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--parallel", default=8)
    parser.add_argument("-n", "--dry-run", default=False,
                        action="store_true")
    parser.add_argument("--project", default=None)
    parser.add_argument("-i", "--ignore", default=False,
                        action="store_true", help="print lines for .gitignore")
    parser.add_argument("-m", "--makefile", default="Makefile")
    parser.add_argument("-c", "--clean", default=False, action="store_true")
    parser.add_argument("-V", "--version", default=False, action="store_true")
    parser.add_argument("-b", "--binary", default=False, action="store_true")
    return parser


def setup_logging():
    logging.basicConfig(
        filename="parse.log",
        level=logging.DEBUG,
        format="[%(levelname)s]%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def main(args):
    if args.ignore:
        print_ignore_lines()
        sys.exit(0)

    if args.version:
        print_version()
        sys.exit(0)

    use_docker = not args.binary

    dependencies = parse_files(".", use_docker)
    dag = Dag(dependencies)
    create_makefile(dag, args.makefile)

    if args.clean:
        clean(args.makefile)
        sys.exit(0)

    create_graph(dag)
    run_query(args.parallel, args.dry_run, args.makefile)
