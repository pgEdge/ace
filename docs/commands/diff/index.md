# ACE diff Commands
ACE diff commands can identify differences between tables and objects in a replication set (repset), schema, or for standalone tables on different nodes of your replication cluster. 

ACE diff functions compare two objects and identify the differences; the output is a report that contains a:

- Summary of compared rows
- Mismatched data details
- Node-specific statistics

If you generate an HTML report, ACE generates an interactive report with:

- Color‑coded difference highlighting
- Expandable row details
- Primary key highlighting
- Missing row indicators

**Common use cases:**

- Performing routine content verification
- Performance‑optimized large table scans
- Focused comparisons between nodes, tables, or schemas

!!! hint

     Running diff commands can be resource-intensive. Experiment with `--block-size`, `--concurrency-factor`, and `--compare-unit-size` to balance runtime and resource usage. Use `--table-filter` for large tables to reduce comparison scope, and generate HTML reports to simplify analysis.
