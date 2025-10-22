# ACE Getting Started

ACE is a powerful tool designed to ensure and maintain consistency across nodes in a pgEdge Distributed Postgres cluster. It helps identify and resolve data inconsistencies, schema differences, and replication configuration mismatches across nodes in a cluster.

Key features of ACE include:

- Table-level data comparison and repair
- Replication set level verification
- Automated repair capabilities
- Schema comparison
- Spock configuration validation

### Known Limitations

* ACE cannot be used on a table without a primary key, because primary keys are the basis for range partitioning, hash calculations, and other critical functions in ACE.

