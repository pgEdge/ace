#!/usr/bin/env python3
#
# Run ACE on two nodes that hold the regression database and check what it
# reports against a reference result computed here with plain SQL.
#
# The reference result for a table with a primary key: read every row on
# each node as (key text, row text) under the same output settings that ACE
# sets on its connections, and count the keys that are on one node only or
# have a different row text. table-diff and mtree table-diff must report
# "match" when the count is 0, and otherwise "diff" with the same number of
# differences. A table without columns or a primary key, a table with other
# key or other columns on the two nodes, or a table on one node only, must
# give the error that names this problem. Every command must end within the
# time limit and without a panic.
#
# schema-diff --ddl-only and --compare=structure have no reference: they
# must report no differences when both nodes run the same major version,
# and must end cleanly in any case. The data mode of schema-diff must give
# the same counts of matching, differing, missing and failed tables as the
# reference results.
#
# Results that differ from the expected ones and are known are listed in
# known.tsv. Any other difference fails the run, and so does a line in
# known.tsv that no longer matches what ACE does.

import argparse
import collections
import concurrent.futures
import json
import os
import re
import shutil
import subprocess  # nosec B404 - the test runs psql and ace
import sys
import time

# The output settings that ACE sends in the startup packet of every
# connection (internal/infra/db/auth.go). The reference reads the rows with
# the same settings, so its text is the text ACE compares.
PINNED = {
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
    "intervalstyle": "postgres",
    "timezone": "UTC",
    "extra_float_digits": "3",
    "bytea_output": "hex",
    "lc_monetary": "C",
    "standard_conforming_strings": "on",
    "search_path": "public",
}

CLUSTER = "rg"
# Outcomes that always fail a check and cannot be listed in known.tsv.
HARD = ("hang", "panic", "killed")
COMMANDS = ("table-diff", "mtree", "schema-diff")
# The names in the command column of results.tsv and known.tsv, by the
# --commands value that runs them.
RESULT_COMMANDS = {
    "table-diff": ("table-diff",),
    "mtree": ("mtree-init", "mtree-build", "mtree-diff", "mtree-drop", "mtree-teardown"),
    "schema-diff": ("schema-ddl", "schema-struct", "schema-data"),
}
NODES = ("n1", "n2")

# User tables and partitioned tables: the qualified name, the schema name as
# it is in the catalog, the primary key columns, if any, whether the table
# is partitioned or has child tables, and the column names in order. Each table
# is one JSON array on one line, so a name with any character in it is read
# back as it is.
TABLES_SQL = r"""
SELECT json_build_array(
         format('%I.%I', n.nspname, c.relname),
         n.nspname,
         coalesce((SELECT json_agg(a.attname ORDER BY k.ord)
                   FROM pg_constraint p,
                        unnest(p.conkey) WITH ORDINALITY AS k(attnum, ord),
                        pg_attribute a
                   WHERE p.conrelid = c.oid AND p.contype = 'p'
                     AND a.attrelid = c.oid AND a.attnum = k.attnum), '[]'::json),
         c.relkind = 'p' OR EXISTS (SELECT 1 FROM pg_inherits i
                                    WHERE i.inhparent = c.oid),
         coalesce((SELECT json_agg(a.attname ORDER BY a.attnum) FROM pg_attribute a
                   WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
                  '[]'::json))
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND c.relpersistence <> 't'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname !~ '^pg_toast'
ORDER BY n.nspname, c.relname;
"""

Table = collections.namedtuple("Table", "schema key has_children columns")
# also: other outcomes that are right too (see reference()).
Ref = collections.namedtuple("Ref", "outcome diffs rows note also", defaults=((),))


class PsqlTimeout(Exception):
    """A query of the test itself did not end in time; the run fails."""


class Node:
    def __init__(self, name, port, psql, timeout):
        self.name = name
        self.port = port
        self.psql_bin = psql
        self.timeout = timeout

    def env(self):
        env = dict(os.environ)
        # The options of a service would win over PGOPTIONS; the connection
        # string below names the server, so no service is used.
        env.pop("PGSERVICE", None)
        env["PGOPTIONS"] = " ".join(
            "-c %s=%s" % (k, v.replace(" ", "\\ "))
            for k, v in PINNED.items() if k != "client_encoding")
        env["PGCLIENTENCODING"] = PINNED["client_encoding"]
        return env

    def psql(self, script, variables=None, check=True):
        """Run a psql script; return (exit code, output bytes, error text).

        The script is a fixed text. Names reach it only as psql variables
        (-v), and the server turns them into identifiers, so no SQL is
        built from strings here.
        """
        # hostaddr is set too: values given here win over PGHOSTADDR and
        # a service file, so no other server can get these queries.
        conninfo = ("host=127.0.0.1 hostaddr=127.0.0.1 port=%d user=postgres dbname=regression"
                    % self.port)
        cmd = [self.psql_bin, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
               "-d", conninfo, "-f", "-"]
        for name, value in (variables or {}).items():
            cmd += ["-v", "%s=%s" % (name, value)]
        # A fixed argument list and no shell; the programs come from the
        # command line of this script.
        try:
            p = subprocess.run(cmd, input=script.encode(), env=self.env(),  # nosec B603 # nosemgrep
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=self.timeout)
        except subprocess.TimeoutExpired as e:
            raise PsqlTimeout("%s: no result after %d s" % (self.name, self.timeout)) from e
        err = p.stderr.decode(errors="replace")
        if check and p.returncode != 0:
            raise RuntimeError("%s: %s" % (self.name, err.strip()))
        return p.returncode, p.stdout, err


def list_tables(node):
    _, out, _ = node.psql(TABLES_SQL)
    tables = {}
    # JSON escapes a line feed inside a string, so only LF ends a record;
    # splitlines() would also split at other line breaks in a name.
    for line in out.split(b"\n"):
        if line:
            name, schema, key, children, columns = json.loads(line)
            tables[name] = Table(schema, key, children, columns)
    return tables


# Every row of a table as (key text, row text). The server builds the COPY
# command from the catalog and runs it with \gexec. ROW(t.*) reads the
# whole row even when the table has a column named t.
REFERENCE_SQL = r"""
SELECT format('COPY (SELECT ROW(%s)::text, ROW(t.*)::text FROM %s t) TO STDOUT',
              string_agg(format('t.%I', a.attname), ', ' ORDER BY k.ord),
              p.conrelid::regclass)
FROM pg_constraint p,
     unnest(p.conkey) WITH ORDINALITY AS k(attnum, ord),
     pg_attribute a
WHERE p.conrelid = :'table'::regclass AND p.contype = 'p'
  AND a.attrelid = p.conrelid AND a.attnum = k.attnum
GROUP BY p.conrelid
\gexec
"""

# Delete the first row of a table in heap order. Triggers and foreign keys
# do not fire with session_replication_role = replica.
DELETE_SQL = r"""
SET session_replication_role = replica;
SELECT format('DELETE FROM ONLY %1$s WHERE ctid = (SELECT min(ctid) FROM ONLY %1$s) RETURNING 1',
              :'table'::regclass)
\gexec
"""


def reference_rows(node, table):
    """Return {key text: [row text, ...]}, or an error string.

    COPY prints one line per row, with newlines and tabs escaped, so a line
    split is safe.
    """
    try:
        rc, out, err = node.psql(REFERENCE_SQL, {"table": table}, check=False)
    except PsqlTimeout as e:
        return str(e)
    if rc != 0:
        return err.strip().splitlines()[-1] if err.strip() else "psql failed"
    rows = collections.defaultdict(list)
    for line in out.split(b"\n"):
        if line:
            k, _, v = line.partition(b"\t")
            rows[k].append(v)
    return rows


# The errors for a table on one node, in the order that gives the reference
# outcome when the two nodes have different ones.
REFUSALS = ("missing", "no-columns", "no-pk")


def refusal(t):
    """Return the error that ACE gives for the table of one node, or None."""
    if t is None:
        return "missing"
    if not t.columns:
        return "no-columns"
    if not t.key:
        return "no-pk"
    return None


def reference(nodes, name, t1, t2):
    # ACE checks the nodes one by one, in no fixed order, and stops at the
    # first node whose table it cannot compare. So the error of either node
    # is right, and no other error is.
    refusals = sorted(set(r for r in (refusal(t1), refusal(t2)) if r), key=REFUSALS.index)
    if refusals:
        note = "on one node only" if refusals[0] == "missing" else ""
        return Ref(refusals[0], None, 0, note, tuple(refusals[1:]))
    if t1.key != t2.key:
        return Ref("schema-mismatch", None, 0, "the primary keys differ")
    if t1.columns != t2.columns:
        # ACE compares rows only when both nodes have the same columns in
        # the same order.
        return Ref("schema-mismatch", None, 0, "the columns differ")
    r1 = reference_rows(nodes[0], name)
    r2 = reference_rows(nodes[1], name)
    if isinstance(r1, str) or isinstance(r2, str):
        # No ACE result can pass this: the test itself must be fixed.
        return Ref("reference-error", None, 0, r1 if isinstance(r1, str) else r2)
    rows = max(sum(len(v) for v in r1.values()), sum(len(v) for v in r2.values()))
    if any(len(v) > 1 for r in (r1, r2) for v in r.values()):
        # Possible only with child tables: a primary key does not cover
        # them, so one key can have several rows. The rows of a key are
        # compared as a multiset; the number of differences is not defined
        # for such a key, so it is not checked.
        same = all(sorted(r1.get(k, [])) == sorted(r2.get(k, [])) for k in set(r1) | set(r2))
        note = "the key is not unique with child tables"
        return Ref("match", 0, rows, note) if same else Ref("diff", None, rows, note)
    diffs = sum(1 for k in set(r1) | set(r2) if r1.get(k) != r2.get(k))
    return Ref("match", 0, rows, "") if diffs == 0 else Ref("diff", diffs, rows, "")


def make_differences(node, t1, t2):
    """Delete one row on node in every second table with a primary key.

    When both nodes run the same version, the tables with a primary key
    usually hold the same rows, and ACE would never get to the code that
    reads the rows that differ. Triggers, foreign keys and rules can block
    or extend a delete; DELETE_SQL turns off the first two, and a delete
    that fails is skipped. A delete that does not end in time is returned
    as a failure of the test. Only a table that really lost a row is
    counted: an empty table gives a delete without a row. The reference
    result is computed after this, so it sees what the delete really did.
    """
    names = sorted(n for n in set(t1) & set(t2) if t1[n].key and t2[n].key)
    changed, timed_out = 0, []
    for name in names[1::2]:
        try:
            rc, out, _ = node.psql(DELETE_SQL, {"table": name}, check=False)
        except PsqlTimeout as e:
            timed_out.append((name, str(e)))
            continue
        changed += rc == 0 and out.strip() == b"1"
    return changed, timed_out


FOUND_RE = re.compile(r"Found (\d+) differences between")
ERROR_RE = re.compile(r"\bERRO\b\s*(.*)")
# The lines that the Go runtime prints after a panic or a fatal error: the
# panic message and the first line of a goroutine dump, each at the start of
# a line. The runtime then ends the process with a nonzero exit status, so
# these lines count only in the output of a run that failed: names in a
# normal output can contain the same text.
PANIC_RE = re.compile(r"^(panic: |goroutine \d+ \[)", re.M)
# The error of a table whose columns or primary key differ on two nodes.
SCHEMA_MISMATCH_RE = re.compile(
    r"table '.*' schema differs between nodes|table schemas don't match between nodes")


def run_ace(ace, workdir, args, timeout):
    """Run one ACE command; return (outcome, differences, detail, output)."""
    start = time.monotonic()
    # ACE reads ACE_CONFIG, ACE_PGSERVICEFILE and PGSERVICEFILE before the
    # files in the work directory; set the ACE ones so that no other
    # configuration or cluster is used.
    workdir = os.path.abspath(workdir)
    env = dict(os.environ,
               ACE_CONFIG=os.path.join(workdir, "ace.yaml"),
               ACE_PGSERVICEFILE=os.path.join(workdir, "pg_service.conf"))
    try:
        # A fixed argument list and no shell, as in Node.psql.
        p = subprocess.run([ace] + args, cwd=workdir, env=env, timeout=timeout,  # nosec B603 # nosemgrep
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except subprocess.TimeoutExpired as e:
        out = (e.output or b"").decode(errors="replace")
        return "hang", None, "no result after %d s" % timeout, out
    out = p.stdout.decode(errors="replace")
    took = "%.1fs" % (time.monotonic() - start)
    if p.returncode != 0 and PANIC_RE.search(out):
        return "panic", None, "", out
    if p.returncode < 0:
        # Ended by a signal: whatever it printed is not a result.
        return "killed", None, "ended by signal %d" % -p.returncode, out
    errors = ERROR_RE.findall(out)
    if p.returncode == 0 and not errors:
        if "TABLES MATCH" in out or "Merkle trees are identical" in out:
            return "match", 0, took, out
        if "TABLES DO NOT MATCH" in out:
            return "diff", sum(int(n) for n in FOUND_RE.findall(out)), took, out
        return "ok", None, took, out
    # table-diff and mtree table-diff exit with 0 when the tables differ,
    # so a verdict with any other exit status belongs to a run that did not
    # end well (for example, it was killed) and is an error.
    lines = out.strip().splitlines()
    detail = errors[-1].strip() if errors else (lines[-1] if lines else "exit %d" % p.returncode)
    if "no primary key found" in detail:
        return "no-pk", None, detail, out
    if re.search(r"table '.*' has no columns on", detail):
        return "no-columns", None, detail, out
    if re.search(r"table '.*' not found on", detail):
        return "missing", None, detail, out
    if SCHEMA_MISMATCH_RE.search(detail):
        return "schema-mismatch", None, detail, out
    return "error", None, detail, out


def classify_schema_ddl(outcome, out):
    """Turn the result of schema-diff --ddl-only into identical/diff/error.

    The command prints a JSON status for each pair of nodes. A run that
    compared no pair (for example, a node was skipped) prints another
    status and still exits with 0, so only IDENTICAL and MISMATCH count.
    """
    if outcome in HARD:
        return outcome
    if outcome != "ok":
        return "error"
    statuses = re.findall(r'"status":\s*"([^"]*)"', out)
    if not statuses or any(s not in ("IDENTICAL", "MISMATCH") for s in statuses):
        return "error"
    return "diff" if "MISMATCH" in statuses else "identical"


def classify_schema_struct(outcome, out):
    """Turn the result of schema-diff --compare=structure into
    identical/diff/error."""
    if outcome in HARD:
        return outcome
    # The command reports differences as an error with this text, so it is
    # read before the general error check.
    if re.search(r"divergence\(s\) found", out):
        return "diff"
    return "identical" if outcome == "ok" else "error"


# The counts that schema-diff prints at the end of a data run, and the
# reference outcome that each table of the schema must give.
SCHEMA_DATA_COUNTS = ("match", "diff", "missing", "error")
SCHEMA_DATA_CLASS = {"no-pk": "error", "no-columns": "error", "schema-mismatch": "error"}
SCHEMA_DATA_RE = {
    "match": r"(\d+) table\(s\) are identical",
    "diff": r"(\d+) table\(s\) have differences",
    "missing": r"(\d+) table\(s\) not present on all nodes",
    "error": r"(\d+) table\(s\) encountered errors",
}


def classify_schema_data(outcome, out):
    """Turn the summary of a schema-diff data run into "match=N diff=N ..."."""
    if outcome in HARD:
        return outcome
    if "summary:" not in out:
        return "error"
    # ok, match and diff mean exit status 0 and no error. The per-table
    # verdicts that the command also prints do not matter here.
    if outcome not in ("ok", "match", "diff"):
        # The only error allowed after the summary is the one that counts
        # the tables that failed. Any other error, for example a stop of
        # the run, fails the check even when the counts are right.
        errors = ERROR_RE.findall(out)
        if not errors or not re.search(r"\d+ table\(s\) failed during Schema diff", errors[-1]):
            return "error"
    counts = []
    for k in SCHEMA_DATA_COUNTS:
        m = re.search(SCHEMA_DATA_RE[k], out)
        counts.append("%s=%s" % (k, m.group(1) if m else "0"))
    return " ".join(counts)


def classify_mtree_build(outcome, out):
    """Tell the refusals of mtree build that are by design from errors."""
    if outcome in HARD + ("ok",):
        return outcome
    if re.search(r"is a partitioned table|has child tables through inheritance", out):
        return "unsupported"
    if re.search(r"has 0 rows on all nodes|has no rows on the reference node", out):
        return "empty"
    return "error"


def load_known(path, cell):
    """Read known.tsv: cell, command, object, connections, result, reason.

    "*" in the cell column matches every case; the object and connections
    columns take exact values only. The result is written as in
    results.tsv, with the number of differences where there is one (for
    example diff:3). Two lines with the same command, object and
    connections for this case are an error. An empty path means no file;
    a path that does not exist is an error.
    """
    known = {}
    if not path:
        return known
    if not os.path.isfile(path):
        sys.exit("known results file %s does not exist" % path)
    with open(path) as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 5:
                sys.exit("%s:%d: expected at least 5 tab-separated fields" % (path, n))
            c, command, obj, conns, outcome = parts[:5]
            if outcome in HARD:
                sys.exit("%s:%d: a %s cannot be a known result" % (path, n, outcome))
            if not any(command in names for names in RESULT_COMMANDS.values()):
                sys.exit("%s:%d: unknown command %r" % (path, n, command))
            if "*" in (obj, conns):
                sys.exit("%s:%d: \"*\" is allowed only in the case column" % (path, n))
            if c in ("*", cell):
                key = (command, obj, conns)
                if key in known:
                    sys.exit("%s:%d: same selector as line %d" % (path, n, known[key][2]))
                known[key] = (outcome, parts[5] if len(parts) > 5 else "", n)
    return known


class Runner:
    def __init__(self, args):
        self.ace = os.path.abspath(args.ace)
        self.workdir = os.path.abspath(args.workdir)
        self.acedir = os.path.join(self.workdir, "ace")
        self.logdir = os.path.join(self.workdir, "logs")
        self.timeout = args.timeout
        self.known = load_known(args.known, args.cell)
        self.used_known = set()
        self.results = []
        self.failures = []
        os.makedirs(self.logdir, exist_ok=True)
        os.makedirs(self.acedir, exist_ok=True)
        with open(os.path.join(self.acedir, "pg_service.conf"), "w") as f:
            f.write("[%s]\ndbname=regression\nuser=postgres\n\n" % CLUSTER)
            for name, port in zip(NODES, (args.port1, args.port2)):
                f.write("[%s.%s]\nhost=127.0.0.1\nport=%d\n\n" % (CLUSTER, name, port))
        config = os.path.join(self.acedir, "ace.yaml")
        if not os.path.exists(config):
            # The output goes to a log, so that the reason of a failure can
            # be read. A failure stops the run. The file is written under a
            # temporary name and gets its real name only when the command
            # succeeds, so a later run never reads a partly written file.
            log = os.path.join(self.logdir, "config-init.log")
            tmp = config + ".tmp"
            if os.path.exists(tmp):
                os.remove(tmp)
            with open(log, "w") as f:
                try:
                    res = subprocess.run([self.ace, "config", "init", "--path", tmp],  # nosec B603 # nosemgrep
                                         cwd=self.acedir, stdout=f, stderr=subprocess.STDOUT,
                                         timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    sys.exit("ace config init did not finish in %d s, see %s" % (self.timeout, log))
            if res.returncode != 0 or not os.path.isfile(tmp):
                sys.exit("ace config init failed with exit status %d, see %s" % (res.returncode, log))
            os.replace(tmp, config)

    def clean(self):
        """Remove what a command left in the work directory, except the config."""
        for entry in os.listdir(self.acedir):
            if entry in ("pg_service.conf", "ace.yaml", "ace_tasks.db"):
                continue
            path = os.path.join(self.acedir, entry)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.remove(path)

    def run(self, cmd, obj, conns, args, expected, exp_diffs=None, note="", classify=None,
            also=()):
        """Run one ACE command, compare the result with the expected one, and
        record it. Return the outcome."""
        outcome, diffs, detail, out = run_ace(self.ace, self.acedir, args, self.timeout)
        self.clean()
        if classify:
            outcome, diffs = classify(outcome, out), None
        ok = check(expected, exp_diffs, outcome, diffs, also)
        status = "ok" if ok else "FAIL"
        exp = expected if exp_diffs is None else "%s:%d" % (expected, exp_diffs)
        got = outcome if diffs is None else "%s:%d" % (outcome, diffs)
        # A line applies when its selector is this run and its result is
        # the whole result, with its count of differences. A panic, a
        # timeout or a kill is never accepted as known.
        k = self.known.get((cmd, obj, str(conns)))
        if not ok and k and k[0] == got and outcome not in HARD:
            self.used_known.add(k[2])
            status = "known"
            detail = "%s: %s" % (k[1], detail) if k[1] else detail
        elif ok and k and expected != "clean":
            status = "STALE"
            detail = "known.tsv:%d lists %s, but the result is as expected now" % (k[2], k[0])
        row = (cmd, obj, str(conns), exp, got, status, note or detail)
        self.results.append(row)
        if status in ("FAIL", "STALE"):
            self.failures.append(row)
            # Two names can give the same text here, so the number of the
            # failure keeps each log apart.
            logname = re.sub(r"[^A-Za-z0-9_.-]", "_", "%s-%s-%s-%d.log" % (
                cmd, obj, conns, len(self.failures)))
            with open(os.path.join(self.logdir, logname), "w") as f:
                f.write("ace " + " ".join(args) + "\n\n" + out)
        if status != "ok":
            print("%-6s %-13s %-42s conns=%-2s expected=%-10s got=%-10s %s" % (
                status, cmd, obj, conns, exp, got, note or detail), flush=True)
        return outcome

    def fail(self, cmd, obj, detail):
        """Record a failure of the test itself, not of an ACE command."""
        row = (cmd, obj, "0", "-", "-", "FAIL", detail)
        self.results.append(row)
        self.failures.append(row)
        print("%-6s %-13s %-42s %s" % ("FAIL", cmd, obj, detail), flush=True)

    def report(self, full):
        """Write results.tsv, print the summary, and return True if the run
        passed. full is False when an option limits the run (see main())."""
        with open(os.path.join(self.workdir, "results.tsv"), "w") as f:
            f.write("command\tobject\tconnections\texpected\tresult\tstatus\tdetail\n")
            # A name can hold a tab or a line break; escape them as COPY
            # does, so that each row keeps its fields.
            for row in self.results:
                f.write("\t".join(tsv_field(v) for v in row) + "\n")
        print()
        for cmd in sorted(set(r[0] for r in self.results)):
            rows = [r for r in self.results if r[0] == cmd]
            status = collections.Counter(r[5] for r in rows)
            outcome = collections.Counter(r[4].split(":")[0] for r in rows)
            print("%-13s %4d runs: %s; results: %s" % (
                cmd, len(rows),
                ", ".join("%s %d" % kv for kv in sorted(status.items())),
                ", ".join("%s %d" % kv for kv in sorted(outcome.items()))))
        # Only a full run covers every line of known.tsv, so only a full run
        # can tell that a line is not used any more.
        if not full:
            print("limited run: known.tsv is not checked for unused lines")
            return not self.failures
        unused = sorted(set(v[2] for v in self.known.values()) - self.used_known)
        for n in unused:
            print("STALE  known.tsv:%d matches nothing in this run" % n)
        return not self.failures and not unused


def tsv_field(value):
    return (value.replace("\\", "\\\\").replace("\t", "\\t")
            .replace("\n", "\\n").replace("\r", "\\r"))


def check(expected, exp_diffs, outcome, diffs, also=()):
    if outcome in HARD:
        return False
    if expected == "clean":
        return outcome in ("identical", "diff")
    if outcome in also:
        return True
    if outcome != expected:
        return False
    return exp_diffs is None or diffs == exp_diffs


def main():
    ap = argparse.ArgumentParser(description="Run ACE on two regression databases.")
    ap.add_argument("--ace", required=True, help="ACE binary")
    ap.add_argument("--psql", required=True, help="psql binary for the reference queries")
    ap.add_argument("--port1", type=int, required=True)
    ap.add_argument("--port2", type=int, required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--cell", required=True, help="name of this case in known.tsv")
    ap.add_argument("--known", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "known.tsv"))
    ap.add_argument("--conns", default="1,0,32",
                    help="values of --max-connections; 0 runs without the option")
    ap.add_argument("--block-size", type=int, default=50, help="table-diff block size")
    ap.add_argument("--timeout", type=int, default=120, help="seconds for one ACE command")
    ap.add_argument("--tables", default="", help="regular expression to select tables")
    ap.add_argument("--commands", default=",".join(COMMANDS))
    ap.add_argument("--no-changes", action="store_true",
                    help="do not delete rows on node n2 before the run")
    args = ap.parse_args()
    # Both nodes are on 127.0.0.1, so one port means one server compared
    # with itself.
    if args.port1 == args.port2:
        ap.error("--port1 and --port2 must differ")

    commands = args.commands.split(",")
    unknown = sorted(set(commands) - set(COMMANDS))
    if unknown:
        ap.error("unknown --commands: %s (choose from %s)" % (", ".join(unknown), ", ".join(COMMANDS)))

    r = Runner(args)
    nodes = (Node(NODES[0], args.port1, args.psql, args.timeout),
             Node(NODES[1], args.port2, args.psql, args.timeout))
    conns = [int(c) for c in args.conns.split(",")]
    # --tables, --commands and --conns exist to repeat a part of the run by
    # hand. Such a limited run checks every command it runs in the same way,
    # but only a full run can tell that a line of known.tsv is not used.
    full = (not args.tables and set(commands) == set(COMMANDS)
            and set(conns) >= set(int(c) for c in ap.get_default("conns").split(",")))
    majors = [int(n.psql("SHOW server_version_num;")[1]) // 10000 for n in nodes]

    t1, t2 = list_tables(nodes[0]), list_tables(nodes[1])
    names = sorted(n for n in set(t1) | set(t2) if re.search(args.tables, n))
    print("PostgreSQL %d and %d; tables: %d on %s, %d on %s, %d selected" % (
        majors[0], majors[1], len(t1), NODES[0], len(t2), NODES[1], len(names)), flush=True)
    if not names:
        sys.exit("no table matches --tables %r" % args.tables)
    # Every selected command must have something to compare; otherwise the
    # run would pass with an empty report.
    # schema-diff takes the schema name as it is in the catalog, not quoted.
    schemas = sorted(set(t1[n].schema for n in names if n in t1 and n in t2))
    if "schema-diff" in commands and not schemas:
        sys.exit("schema-diff: no selected table is on both nodes")
    # mtree builds a tree only for a table whose rows the reference compares:
    # on both nodes, with the same primary key and columns (see reference()).
    if "mtree" in commands and not any(
            n in t1 and n in t2 and t1[n].key and t1[n].key == t2[n].key
            and t1[n].columns == t2[n].columns for n in names):
        sys.exit("mtree: no selected table with the same primary key and columns is on both nodes")
    # The rows are deleted in the same tables whatever --tables selects, so
    # that a run on some tables sees the same data as a full run.
    if not args.no_changes:
        changed, timed_out = make_differences(nodes[1], t1, t2)
        print("deleted one row on %s in %d tables" % (NODES[1], changed), flush=True)
        for name, detail in timed_out:
            r.fail("delete", name, detail)
        # Without a deleted row, no table differs because of this step, and
        # the code that reads the rows that differ may never run.
        if not changed:
            r.fail("delete", NODES[1], "no row was deleted; use --no-changes to run without deletes")

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        refs = dict(zip(names, pool.map(
            lambda n: reference(nodes, n, t1.get(n), t2.get(n)), names)))
    # A reference that could not be read fails the run whatever commands
    # are selected; the commands then skip the table or fail on it.
    for name in names:
        if refs[name].outcome == "reference-error":
            r.fail("reference", name, refs[name].note)

    if "table-diff" in commands:
        for name in names:
            ref = refs[name]
            for c in conns:
                cmd = ["table-diff", "-b", str(args.block_size)]
                cmd += ["-M", str(c)] if c else []
                r.run("table-diff", name, c, cmd + [CLUSTER, name],
                      ref.outcome, ref.diffs, ref.note, also=ref.also)

    if "mtree" in commands:
        # mtree table-diff needs one connection for the tree transaction
        # and at least one more, so 1 becomes 2.
        mconns = sorted(set(max(c, 2) if c else 0 for c in conns), key=lambda c: (c != 2, c))
        r.run("mtree-init", CLUSTER, 0, ["mtree", "init", CLUSTER], "ok")
        for name in names:
            ref = refs[name]
            if ref.outcome not in ("match", "diff"):
                continue
            # A table with child tables gets no tree; an empty table gets
            # none either. Blocks of 10 rows give real trees on small tables.
            if t1[name].has_children or t2[name].has_children:
                expected = "unsupported"
            elif ref.rows == 0:
                expected = "empty"
            else:
                expected = "ok"
            # Each connection limit builds its own tree and compares with
            # it; the tree is removed before the next build. mtree build
            # needs at least 3 connections (a cutting worker, a hash worker
            # and the tree transaction), so the smallest case builds with 3
            # and compares with 2.
            for c in mconns:
                bc = max(c, 3) if c else 0
                build_limit = ["-M", str(bc)] if bc else []
                built = r.run("mtree-build", name, bc,
                              ["mtree", "build", "-b", "10", "-B"] + build_limit + [CLUSTER, name],
                              expected, classify=classify_mtree_build)
                if built != "ok":
                    continue
                limit = ["-M", str(c)] if c else []
                r.run("mtree-diff", name, c, ["mtree", "table-diff"] + limit + [CLUSTER, name],
                      ref.outcome, ref.diffs)
                r.run("mtree-drop", name, c, ["mtree", "teardown-table", CLUSTER, name], "ok")
        r.run("mtree-teardown", CLUSTER, 0, ["mtree", "teardown", CLUSTER], "ok")

    if "schema-diff" in commands:
        expected = "identical" if majors[0] == majors[1] else "clean"
        # The schema name is passed as it is and can start with "-", so "--"
        # ends the options before it.
        for s in schemas:
            r.run("schema-ddl", s, 0, ["schema-diff", "--ddl-only", "--", CLUSTER, s],
                  expected, classify=classify_schema_ddl)
            r.run("schema-struct", s, 0, ["schema-diff", "--compare=structure", "--", CLUSTER, s],
                  expected, classify=classify_schema_struct)
        # The data mode runs table-diff on every table of the schema and
        # prints how many tables match, differ, are on one node only, or
        # fail. Each count must agree with the reference results.
        # schema-diff compares every table of the schema, whatever --tables
        # selects, so the counts can be checked only without --tables.
        # A public table on one node only counts as missing, so the run
        # needs no table that is on both nodes.
        if not args.tables and any((t1.get(n) or t2.get(n)).schema == "public" for n in names):
            want = collections.Counter(
                SCHEMA_DATA_CLASS.get(refs[n].outcome, refs[n].outcome)
                for n in names if (t1.get(n) or t2.get(n)).schema == "public")
            expected = " ".join("%s=%d" % (k, want[k]) for k in SCHEMA_DATA_COUNTS)
            for c in conns:
                limit = ["-M", str(c)] if c else []
                r.run("schema-data", "public", c, ["schema-diff"] + limit + [CLUSTER, "public"],
                      expected, classify=classify_schema_data)

    sys.exit(0 if r.report(full) else 1)


if __name__ == "__main__":
    main()
