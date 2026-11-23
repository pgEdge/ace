#!/usr/bin/env python3
#
# Print the lines of a plain text dump (from stdin) in a form that does not
# depend on the order of the columns of a table, sorted.
#
# pg_dump does not keep the column order of a table that inherits columns
# from a parent table: the restored table gets the inherited columns
# first. Its CREATE TABLE command lists the same columns in another order,
# so the comma at the end of a column line can move, and its COPY command
# lists the columns and the values of each row in another order. This
# script puts the columns of each COPY command and the values of its rows
# in the order of the column names, and removes the comma at the end of
# each line. Before PostgreSQL 18, pg_dump also orders some objects with
# the same name by OID, so the order of the lines can change; the output is
# sorted for this reason.
#
# The \restrict and \unrestrict lines hold a random key that is new in each
# dump, so they are left out.

import re
import sys

RESTRICT_RE = re.compile(r"^\\(un)?restrict ")


def split_names(text):
    """Split a list of identifiers at the commas outside double quotes."""
    names, cur, quoted = [], [], False
    for ch in text:
        if ch == '"':
            quoted = not quoted
        if ch == "," and not quoted:
            names.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    names.append("".join(cur).strip())
    return names


def copy_columns(line):
    """Return (prefix, column names) of a COPY ... FROM stdin; line, or None."""
    if not (line.startswith("COPY ") and line.endswith(") FROM stdin;")):
        return None
    quoted = False
    for i, ch in enumerate(line):
        if ch == '"':
            quoted = not quoted
        elif ch == "(" and not quoted:
            return line[:i], split_names(line[i + 1:-len(") FROM stdin;")])
    return None


def main():
    out = []
    order = None
    data = sys.stdin.buffer.read().decode("utf-8", errors="surrogateescape")
    for line in data.split("\n"):
        if order is not None:
            if line == "\\.":
                order = None
            else:
                # COPY escapes a tab inside a value, so a tab always
                # separates two values.
                values = line.split("\t")
                if len(values) == len(order):
                    line = "\t".join(values[i] for i in order)
            out.append(line)
            continue
        if RESTRICT_RE.match(line):
            continue
        cols = copy_columns(line)
        if cols:
            prefix, names = cols
            order = sorted(range(len(names)), key=lambda i: names[i])
            line = "%s(%s) FROM stdin;" % (prefix, ", ".join(names[i] for i in order))
        out.append(line.rstrip(","))
    out.sort()
    sys.stdout.buffer.write("\n".join(out).encode("utf-8", errors="surrogateescape") + b"\n")


if __name__ == "__main__":
    main()
