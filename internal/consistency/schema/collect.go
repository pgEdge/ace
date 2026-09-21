// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2026, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package schema

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
)

// CollectSnapshot reads every structural property this package knows how to
// compare, for the given tables, on one node.
//
// Everything is read inside a single REPEATABLE READ, read-only transaction:
// open it, read within it, done. What CollectSnapshot returns is what was
// true on this node at the moment the transaction started.
//
// schemaName is a single PostgreSQL namespace; tables must already be
// unqualified names within it. Multi-schema scopes are not handled yet.
func CollectSnapshot(ctx context.Context, pool *pgxpool.Pool, nodeName, schemaName string, tables []string) (Snapshot, error) {
	snap := Snapshot{
		Node:         nodeName,
		Objects:      make(map[ObjectID]Object),
		TableColumns: make(map[ObjectID][]string),
	}
	if len(tables) == 0 {
		return snap, nil
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("could not open a REPEATABLE READ snapshot on %s: %w", nodeName, err)
	}
	// A read-only transaction that is never committed is always safe to roll
	// back, including after a successful read: there is nothing to persist.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := pinDeparseSettings(ctx, tx, nodeName); err != nil {
		return Snapshot{}, err
	}

	columnsByTable, err := queries.GetColumnDescriptors(ctx, tx, schemaName, tables)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading column descriptors on %s: %w", nodeName, err)
	}
	keysByTable, err := queries.GetReplicaIdentityKey(ctx, tx, schemaName, tables)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading replica identity keys on %s: %w", nodeName, err)
	}
	constraintsByTable, err := queries.GetConstraintDescriptors(ctx, tx, schemaName, tables)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading constraint descriptors on %s: %w", nodeName, err)
	}
	partitionsByTable, err := queries.GetPartitionDescriptors(ctx, tx, schemaName, tables)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading partition descriptors on %s: %w", nodeName, err)
	}

	domains, ranges, composites, enums, err := fetchReferencedTypeDescriptors(ctx, tx, nodeName, columnsByTable)
	if err != nil {
		return Snapshot{}, err
	}

	// The database's own collation settings, which every column that does
	// not name a collation silently inherits. Collected here rather than
	// per column because that inheritance is invisible in pg_attribute -
	// see GetDatabaseLocale.
	locale, err := queries.GetDatabaseLocale(ctx, tx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("reading database locale on %s: %w", nodeName, err)
	}

	// Every identifier this snapshot will print is quoted in one round trip,
	// up front. See QuoteIdentifiers' doc comment.
	quotedOf, err := queries.QuoteIdentifiers(ctx, tx, identifiersToQuote(schemaName, tables, columnsByTable, domains, ranges, composites, enums))
	if err != nil {
		return Snapshot{}, fmt.Errorf("quoting identifiers for display on %s: %w", nodeName, err)
	}

	buildReferencedTypeObjects(quotedOf, domains, ranges, composites, enums, snap.Objects)

	localeObj := Object{Kind: "database", Name: locale.Name, Properties: []Property{
		{Name: "lc_collate", Value: locale.Collate},
		{Name: "lc_ctype", Value: locale.Ctype},
		{Name: "locale_provider", Value: locale.Provider},
		{Name: "locale", Value: locale.Locale},
	}}
	sortProperties(localeObj.Properties)
	snap.Objects[DatabaseID()] = localeObj

	for _, table := range tables {
		// A table the catalog did not return was dropped between the caller
		// resolving its scope and this snapshot. Writing no Object lets
		// Compare report it once, as absent on the table, rather than once
		// per column. partitionsByTable is the existence test:
		// GetPartitionDescriptors filters on schema and name only, so it
		// holds one row per relation that exists.
		if _, exists := partitionsByTable[table]; !exists {
			continue
		}

		tableID := TableID(schemaName, table)
		qualified := qualify(quotedOf, schemaName, table)

		// Table-level object: partition information only, written even when
		// that is empty so a table that exists is never taken for a missing
		// one.
		tableObj := Object{Kind: "table", Name: qualified}
		if p, ok := partitionsByTable[table]; ok {
			tableObj.Properties = append(tableObj.Properties,
				Property{Name: "partition_bound", Value: p.PartitionBound},
				Property{Name: "partition_key", Value: p.PartitionKey},
			)
		}
		sortProperties(tableObj.Properties)
		snap.Objects[tableID] = tableObj

		for _, c := range columnsByTable[table] {
			colName := qualify(quotedOf, schemaName, table, c.Name)
			snap.TableColumns[tableID] = append(snap.TableColumns[tableID], c.Name)
			// A collation is identified the same way a type is, by
			// (namespace, name); the provider and recorded version follow.
			// A column with no collation of its own inherits the database's,
			// which is compared once as the "database" Object rather than
			// repeated here, since pg_attribute cannot show it.
			collation := ""
			if c.CollName != "" {
				collation = fmt.Sprintf("%s.%s/%s/%s",
					c.CollNamespace, c.CollName, c.CollProvider, c.CollVersion)
			}
			colObj := Object{
				Kind: "column",
				Name: colName,
				Properties: []Property{
					{Name: "type", Value: c.TypeText},
					// Type identity is (type_namespace, type_name, type_mod),
					// portable across nodes and PostgreSQL versions — never
					// the type's OID. See GetColumnDescriptors' doc comment.
					{Name: "type_namespace", Value: c.TypeNamespace},
					{Name: "type_name", Value: c.TypeName},
					{Name: "type_kind", Value: c.TypeKind},
					{Name: "type_mod", Value: strconv.FormatInt(int64(c.TypeMod), 10)},
					{Name: "notnull", Value: strconv.FormatBool(c.NotNull)},
					{Name: "identity", Value: c.Identity},
					{Name: "generated", Value: c.Generated},
					{Name: "options", Value: c.Options},
					{Name: "collation", Value: collation},
					{Name: "default", Value: c.DefaultExpr},
				},
			}
			sortProperties(colObj.Properties)
			snap.Objects[ColumnID(schemaName, table, c.Name)] = colObj
		}

		keyObj := Object{Kind: "key", Name: qualified}
		if k, ok := keysByTable[table]; ok {
			keyObj.Properties = []Property{
				{Name: "replica_identity", Value: k.ReplicaIdentity},
				// joinList, not a comma: a column named "a,b" would
				// otherwise make the key (a,b) and the key ("a,b")
				// indistinguishable.
				{Name: "key_columns", Value: joinList(k.KeyColumns)},
				{Name: "key_opclasses", Value: joinList(k.KeyOpclasses)},
			}
		}
		sortProperties(keyObj.Properties)
		snap.Objects[KeyID(schemaName, table)] = keyObj

		// Constraints are a set, not a keyed collection: PostgreSQL invents
		// names for unnamed constraints, so there is no stable per-name slot
		// to compare across nodes. All of a table's constraints live as
		// same-named "constraint" properties on one Object, ordered by
		// definition text via sortProperties.
		constraintObj := Object{Kind: "constraint", Name: qualified}
		for _, c := range constraintsByTable[table] {
			constraintObj.Properties = append(constraintObj.Properties, Property{
				Name:  "constraint",
				Value: fmt.Sprintf("%s|deferrable=%t|validated=%t|%s", c.Type, c.Deferrable, c.Validated, c.Definition),
			})
		}
		sortProperties(constraintObj.Properties)
		snap.Objects[ConstraintID(schemaName, table)] = constraintObj
	}

	return snap, nil
}

// fetchReferencedTypeDescriptors finds every domain, range, composite and
// enum type the compared columns depend on, however indirectly, and reads
// each one's full descriptor.
//
// This lets Compare notice that two columns declaring "the same" domain by
// name actually disagree about what it constrains (a different CHECK, a
// different base type) — comparing by name and definition, not by OID,
// keeps this stable across independently initdb'd nodes.
//
// Discovery walks the type graph to a fixed point rather than looking only
// at each column's own typtype, because most user-defined types are not
// reached in one step: an array type is a base type in its own right, so a
// "status[]" column names no enum at all, and an enum can sit inside a
// composite's attribute or under a chain of domains without any column
// mentioning it. See resolveTypeClosure and GetTypeReferences.
//
// The OIDs gathered here are this node's own, valid only for further
// catalog lookups on this connection — never sent for cross-node
// comparison. Only the portable fields the queries return (namespace/name/
// kind/typmod strings, sorted constraint text, ordered labels) reach the
// Snapshot. Building Objects from these descriptors is
// buildReferencedTypeObjects' job; fetching is kept separate because every
// identifier they mention must be quoted before any Object is built (see
// identifiersToQuote).
func fetchReferencedTypeDescriptors(ctx context.Context, tx pgx.Tx, nodeName string, columnsByTable map[string][]queries.ColumnDescriptor) (
	domains map[uint32]queries.DomainDescriptor,
	ranges map[uint32]queries.RangeDescriptor,
	composites map[uint32]queries.CompositeDescriptor,
	enums map[uint32]queries.EnumDescriptor,
	err error,
) {
	seed := make([]uint32, 0, len(columnsByTable))
	seen := make(map[uint32]bool)
	for _, cols := range columnsByTable {
		for _, c := range cols {
			if seen[c.TypeOID] {
				continue
			}
			seen[c.TypeOID] = true
			seed = append(seed, c.TypeOID)
		}
	}

	kinds, err := resolveTypeClosure(ctx, tx, nodeName, seed)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var domainOIDs, rangeOIDs, compositeOIDs, enumOIDs []uint32
	for _, oid := range sortedOIDs(kinds) {
		switch kinds[oid] {
		case "d":
			domainOIDs = append(domainOIDs, oid)
		case "r":
			rangeOIDs = append(rangeOIDs, oid)
		case "c":
			compositeOIDs = append(compositeOIDs, oid)
		case "e":
			enumOIDs = append(enumOIDs, oid)
		}
	}

	domains, err = queries.GetDomainDescriptors(ctx, tx, domainOIDs)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("reading domain descriptors on %s: %w", nodeName, err)
	}
	ranges, err = queries.GetRangeDescriptors(ctx, tx, rangeOIDs)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("reading range descriptors on %s: %w", nodeName, err)
	}
	composites, err = queries.GetCompositeAttributes(ctx, tx, compositeOIDs)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("reading composite attributes on %s: %w", nodeName, err)
	}
	enums, err = queries.GetEnumLabels(ctx, tx, enumOIDs)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("reading enum labels on %s: %w", nodeName, err)
	}
	return domains, ranges, composites, enums, nil
}

// resolveTypeClosure expands seed into every type reachable from it,
// returning each reachable type's typtype keyed by this node's OID.
//
// The walk is breadth-first and asks about a given OID exactly once, which
// is what makes it terminate: an OID is recorded as requested before the
// round that asks about it, so a type PostgreSQL declines to report (it
// should report all of them) cannot be re-queued forever. Since every OID
// is visited once, the number of rounds is bounded by the depth of the type
// graph — three or four in practice, an array of a domain over a composite
// being about as deep as real schemas go.
func resolveTypeClosure(ctx context.Context, tx pgx.Tx, nodeName string, seed []uint32) (map[uint32]string, error) {
	kinds := make(map[uint32]string, len(seed))
	requested := make(map[uint32]bool, len(seed))

	pending := make([]uint32, 0, len(seed))
	for _, oid := range seed {
		if oid != 0 && !requested[oid] {
			requested[oid] = true
			pending = append(pending, oid)
		}
	}

	for len(pending) > 0 {
		refs, err := queries.GetTypeReferences(ctx, tx, pending)
		if err != nil {
			return nil, fmt.Errorf("resolving referenced types on %s: %w", nodeName, err)
		}

		var next []uint32
		for _, ref := range refs {
			kinds[ref.OID] = ref.Kind
			for _, dep := range ref.Refs {
				if dep == 0 || requested[dep] {
					continue
				}
				requested[dep] = true
				next = append(next, dep)
			}
		}
		// refs is a map, so next comes out in Go's map order; sorting keeps
		// the query parameters, and anything a log or test reads from them,
		// stable from run to run.
		sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
		pending = next
	}

	return kinds, nil
}

// sortedOIDs returns m's keys in ascending order, so every batch of OIDs
// this file hands to a query is built the same way on both nodes.
func sortedOIDs(m map[uint32]string) []uint32 {
	out := make([]uint32, 0, len(m))
	for oid := range m {
		out = append(out, oid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// buildReferencedTypeObjects turns fetchReferencedTypeDescriptors' result
// into one Object per distinct type, added to objects.
func buildReferencedTypeObjects(
	quotedOf map[string]string,
	domains map[uint32]queries.DomainDescriptor,
	ranges map[uint32]queries.RangeDescriptor,
	composites map[uint32]queries.CompositeDescriptor,
	enums map[uint32]queries.EnumDescriptor,
	objects map[ObjectID]Object,
) {
	for _, d := range domains {
		qualified := qualify(quotedOf, d.Namespace, d.Name)
		props := []Property{
			// Namespace, name and kind are kept as separate properties so
			// compareDomainObject can feed them straight into classifyType.
			{Name: "basetype_namespace", Value: d.BaseTypeNamespace},
			{Name: "basetype_name", Value: d.BaseTypeName},
			{Name: "basetype_kind", Value: d.BaseTypeKind},
			{Name: "basetypmod", Value: strconv.FormatInt(int64(d.BaseTypeMod), 10)},
			// Display only; the three fields above are what gets compared.
			{Name: "basetype_text", Value: d.BaseTypeText},
			{Name: "notnull", Value: strconv.FormatBool(d.NotNull)},
			{Name: "default", Value: d.Default},
		}
		for _, chk := range d.Checks {
			props = append(props, Property{Name: "check", Value: chk})
		}
		sortProperties(props)
		objects[TypeID("domain", d.Namespace, d.Name)] = Object{Kind: "domain", Name: qualified, Properties: props}
	}

	for _, r := range ranges {
		qualified := qualify(quotedOf, r.Namespace, r.Name)
		props := []Property{
			{Name: "subtype", Value: qualify(quotedOf, r.SubtypeNamespace, r.SubtypeName)},
			{Name: "subtype_kind", Value: r.SubtypeKind},
			{Name: "collation", Value: r.Collation},
			{Name: "opclass", Value: r.Opclass},
			{Name: "canonical", Value: r.Canonical},
			{Name: "subtype_diff", Value: r.SubtypeDiff},
		}
		sortProperties(props)
		objects[TypeID("range", r.Namespace, r.Name)] = Object{Kind: "range", Name: qualified, Properties: props}
	}

	for _, d := range composites {
		qualified := qualify(quotedOf, d.Namespace, d.Name)
		var props []Property
		for _, a := range d.Attributes {
			props = append(props,
				Property{
					// The attribute's own attnum, not its position in the
					// slice: dropping one attribute must not renumber the
					// ones after it, or a single DROP ATTRIBUTE on one node
					// makes every later attribute look changed too.
					Name: fmt.Sprintf("attr:%04d:%s", a.AttNum, a.Name),
					// Packed (see packAttr) so compareCompositeObject can
					// split it apart and classify the type the same way a
					// column's type is classified (e.g. int4 -> int8 as
					// RankNarrowed, not a blanket incompatible).
					Value: packAttr(a.TypeNamespace, a.TypeName, a.TypeKind, a.TypeMod, a.Collation),
				},
				// Display only; paired with the property above by attnum and
				// attribute name.
				Property{
					Name:  fmt.Sprintf("attrtext:%04d:%s", a.AttNum, a.Name),
					Value: attributeText(a.TypeText, a.Collation),
				},
			)
		}
		// Not sorted: attribute order is part of a composite type's
		// structural identity, so the "%04d" attnum keeps this Object's
		// property order attnum-stable.
		objects[TypeID("composite", d.Namespace, d.Name)] = Object{Kind: "composite", Name: qualified, Properties: props}
	}

	for _, d := range enums {
		objects[TypeID("enum", d.Namespace, d.Name)] = Object{
			Kind: "enum", Name: qualify(quotedOf, d.Namespace, d.Name),
			// joinList, not a comma: a label may contain one, and
			// ('a,b','c') must not compare equal to ('a','b,c').
			Properties: []Property{{Name: "labels", Value: joinList(d.Labels)}},
		}
	}
}

// identifiersToQuote collects every distinct identifier this snapshot will
// print, for one QuoteIdentifiers round trip. Missing one here is a quoting
// bug, not a correctness bug: qualify falls back to the raw, unescaped name.
func identifiersToQuote(
	schemaName string,
	tables []string,
	columnsByTable map[string][]queries.ColumnDescriptor,
	domains map[uint32]queries.DomainDescriptor,
	ranges map[uint32]queries.RangeDescriptor,
	composites map[uint32]queries.CompositeDescriptor,
	enums map[uint32]queries.EnumDescriptor,
) []string {
	seen := map[string]bool{schemaName: true}
	for _, table := range tables {
		seen[table] = true
		for _, c := range columnsByTable[table] {
			seen[c.Name] = true
		}
	}
	for _, d := range domains {
		seen[d.Namespace] = true
		seen[d.Name] = true
	}
	for _, r := range ranges {
		seen[r.Namespace] = true
		seen[r.Name] = true
		seen[r.SubtypeNamespace] = true
		seen[r.SubtypeName] = true
	}
	for _, d := range composites {
		seen[d.Namespace] = true
		seen[d.Name] = true
		for _, a := range d.Attributes {
			seen[a.Name] = true
		}
	}
	for _, d := range enums {
		seen[d.Namespace] = true
		seen[d.Name] = true
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	return names
}

// deparseSettings are pinned for the life of every snapshot transaction.
//
// Much of what this package collects is text rendered by the reading
// session (pg_get_constraintdef, pg_get_expr, format_type), and that
// rendering depends on session settings — a differing DateStyle alone can
// turn a byte-identical schema into several spurious divergences, purely in
// how date literals are spelled. Pinning these settings, the same way
// pg_dump does before deparsing, makes the collected text a function of the
// catalog alone.
//
// search_path is pinned too, so any name a deparse routine chooses to
// qualify is decided the same way on both nodes — every query in this
// transaction schema-qualifies what it reads, against pg_catalog
// explicitly.
var deparseSettings = []string{
	"SET LOCAL DateStyle = 'ISO, YMD'",
	"SET LOCAL IntervalStyle = 'postgres'",
	"SET LOCAL TimeZone = 'UTC'",
	"SET LOCAL bytea_output = 'hex'",
	// money's output function formats through lc_monetary, so a default or
	// CHECK holding a money constant deparses differently under a different
	// monetary locale.
	"SET LOCAL lc_monetary = 'C'",
	"SET LOCAL extra_float_digits = 3",
	"SET LOCAL standard_conforming_strings = on",
	"SET LOCAL search_path = pg_catalog",
}

// pinDeparseSettings applies deparseSettings inside the snapshot
// transaction. SET LOCAL is allowed in a READ ONLY transaction and reverts
// when the transaction ends, so the connection is left unchanged.
func pinDeparseSettings(ctx context.Context, tx pgx.Tx, nodeName string) error {
	for _, stmt := range deparseSettings {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("pinning deparse settings on %s (%s): %w", nodeName, stmt, err)
		}
	}
	return nil
}

// Qualify is qualify, exported for callers outside this package that have
// an identifier to print and a quotedOf map from queries.QuoteIdentifiers -
// internal/consistency/diff naming a table that exists on only some nodes,
// for one. It is the same function so that one schema-diff report spells a
// given name exactly one way.
func Qualify(quotedOf map[string]string, parts ...string) string {
	return qualify(quotedOf, parts...)
}

// qualify joins identifiers for a person to read, each rendered through
// quotedOf (from queries.QuoteIdentifiers) exactly as PostgreSQL itself
// would write it back — quoted and case-preserved wherever an unquoted
// spelling would mean something else. This keeps a dot inside a quoted
// identifier from being misread as a component separator. See ObjectID's
// doc comment for the comparison-key half of this problem.
//
// A part missing from quotedOf falls back to the raw, unquoted part rather
// than failing the whole snapshot over a display string.
func qualify(quotedOf map[string]string, parts ...string) string {
	out := make([]string, len(parts))
	for i, p := range parts {
		if q, ok := quotedOf[p]; ok {
			out[i] = q
		} else {
			out[i] = p
		}
	}
	return strings.Join(out, ".")
}

// joinList joins values that are compared as one string but must stay
// individually distinguishable — enum labels, key column names. A comma
// cannot do this: it is a legal character in both, so ("a,b", "c") and
// ("a", "b,c") would join to the same string and compare equal. The same
// reasoning as ObjectID's, for values rather than identifiers.
//
// Each value is written as its own byte length, then ":", then its bytes —
// a netstring, not a delimiter. An enum label is a string literal, not an
// identifier, so nothing stops a user from writing one that contains
// whatever byte a delimiter-based join would pick to keep values apart;
// counting bytes up front needs no such byte to be off limits, so the join
// is unambiguous for every value, not just the realistic ones.
func joinList(values []string) string {
	var b strings.Builder
	for _, v := range values {
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String()
}

// splitList reverses joinList. ok is false for a string that is not validly
// encoded — corrupt input, or a value that never went through joinList.
func splitList(joined string) (values []string, ok bool) {
	for len(joined) > 0 {
		sep := strings.IndexByte(joined, ':')
		if sep < 0 {
			return nil, false
		}
		n, err := strconv.Atoi(joined[:sep])
		if err != nil || n < 0 {
			return nil, false
		}
		rest := joined[sep+1:]
		if len(rest) < n {
			return nil, false
		}
		values = append(values, rest[:n])
		joined = rest[n:]
	}
	return values, true
}

// packAttr packs one composite attribute's type identity into a single
// string, the same way joinList packs a list — as length-prefixed fields,
// not delimiter-joined ones, so a collation or type name containing
// whatever character a delimiter would have used still round-trips.
func packAttr(typeNamespace, typeName, typeKind string, typeMod int32, collation string) string {
	return joinList([]string{
		typeNamespace,
		typeName,
		typeKind,
		strconv.FormatInt(int64(typeMod), 10),
		collation,
	})
}

// unpackAttr reverses packAttr. ok is false for a value that did not come
// from packAttr.
func unpackAttr(packed string) (typeNamespace, typeName, typeKind string, typeMod int32, collation string, ok bool) {
	fields, ok := splitList(packed)
	if !ok || len(fields) != 5 {
		return "", "", "", 0, "", false
	}
	mod, err := strconv.ParseInt(fields[3], 10, 32)
	if err != nil {
		return "", "", "", 0, "", false
	}
	return fields[0], fields[1], fields[2], int32(mod), fields[4], true
}

// attributeText renders one composite attribute for a person: its type as
// PostgreSQL itself would print it, plus the collation when the attribute
// carries one.
func attributeText(typeText, collation string) string {
	if collation == "" {
		return typeText
	}
	return typeText + " collate " + collation
}

// sortProperties orders a table's properties deterministically by (Name,
// Value). For columns, keys and the table object, Names are unique, so this
// is effectively a sort by Name. For constraints, every entry shares the
// Name "constraint", so this falls back to sorting by Value — i.e. by
// constraint definition, not by PostgreSQL's invented name.
func sortProperties(props []Property) {
	sort.Slice(props, func(i, j int) bool {
		if props[i].Name != props[j].Name {
			return props[i].Name < props[j].Name
		}
		return props[i].Value < props[j].Value
	})
}
