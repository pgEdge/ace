// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pgedge/ace/pkg/types"
)

// htmlPairCount captures the diff counts grouped by node pair for the report summary.
type htmlPairCount struct {
	Name  string
	Count string
}

func writeHTMLDiffReport(diffResult types.DiffOutput, jsonFilePath string) (string, error) {
	if jsonFilePath == "" {
		return "", nil
	}

	htmlPath := strings.TrimSuffix(jsonFilePath, filepath.Ext(jsonFilePath)) + ".html"
	summary := diffResult.Summary

	type summaryItem struct {
		Label string
		Value string
	}

	type summaryData struct {
		Items     []summaryItem
		Breakdown []htmlPairCount
	}

	type cell struct {
		Column     string
		IsKey      bool
		NodeAHTML  template.HTML
		NodeAClass string
		NodeBHTML  template.HTML
		NodeBClass string
	}

	type row struct {
		Cells []cell
	}

	type missingGroup struct {
		Title         string
		Rows          []row
		DividerBefore bool
	}

	type pairSection struct {
		NodeA      string
		NodeB      string
		DiffCount  string
		ValueDiffs []row
		Missing    []missingGroup
		HasDiffs   bool
	}

	type reportData struct {
		Summary summaryData
		Pairs   []pairSection
	}

	summaryItems := []summaryItem{
		{Label: "Table", Value: fmt.Sprintf("%s.%s", summary.Schema, summary.Table)},
		{Label: "Nodes", Value: strings.Join(summary.Nodes, ", ")},
		{Label: "Primary Key", Value: formatPrimaryKey(summary.PrimaryKey)},
		{Label: "Total Differences", Value: formatInt64WithCommas(totalDiffs(summary.DiffRowsCount))},
		{Label: "Total Rows Checked", Value: formatInt64WithCommas(summary.TotalRowsChecked)},
		{Label: "Initial Ranges", Value: formatInt64WithCommas(int64(summary.InitialRangesCount))},
		{Label: "Mismatched Ranges", Value: formatInt64WithCommas(int64(summary.MismatchedRangesCount))},
		{Label: "Block Size", Value: formatInt64WithCommas(int64(summary.BlockSize))},
		{Label: "Compare Unit Size", Value: formatInt64WithCommas(int64(summary.CompareUnitSize))},
		{Label: "Concurrency Factor", Value: strconv.Itoa(summary.ConcurrencyFactor)},
		{Label: "Time Taken", Value: formatDurationHuman(summary.TimeTaken)},
		{Label: "Start Time", Value: formatTimestampHuman(summary.StartTime)},
		{Label: "End Time", Value: formatTimestampHuman(summary.EndTime)},
	}

	if summary.MaxDiffRows > 0 {
		summaryItems = append(summaryItems, summaryItem{
			Label: "Max Diff Rows",
			Value: formatInt64WithCommas(summary.MaxDiffRows),
		})
		if summary.DiffRowLimitReached {
			summaryItems = append(summaryItems, summaryItem{
				Label: "Stopped Early",
				Value: "yes (max_diff_rows limit)",
			})
		}
	}

	var filteredItems []summaryItem
	for _, item := range summaryItems {
		if item.Value != "" && item.Value != "0" {
			filteredItems = append(filteredItems, item)
		}
	}

	pkSet := make(map[string]struct{}, len(summary.PrimaryKey))
	for _, col := range summary.PrimaryKey {
		pkSet[col] = struct{}{}
	}

	pairKeys := make([]string, 0, len(diffResult.NodeDiffs))
	for key := range diffResult.NodeDiffs {
		pairKeys = append(pairKeys, key)
	}
	sort.Strings(pairKeys)

	pairs := make([]pairSection, 0, len(pairKeys))

	for _, pairKey := range pairKeys {
		nodeDiff := diffResult.NodeDiffs[pairKey]
		nodeNames := strings.Split(pairKey, "/")
		if len(nodeNames) != 2 {
			nodeNames = nodeNames[:0]
			for name := range nodeDiff.Rows {
				nodeNames = append(nodeNames, name)
			}
			sort.Strings(nodeNames)
			if len(nodeNames) < 2 {
				continue
			}
		}

		nodeA := nodeNames[0]
		nodeB := nodeNames[1]
		rowsA := nodeDiff.Rows[nodeA]
		rowsB := nodeDiff.Rows[nodeB]

		if len(rowsA) == 0 && len(rowsB) == 0 {
			continue
		}

		columns := collectColumnsInOrder(summary.PrimaryKey, rowsA, rowsB)

		rowMapA := make(map[string]types.OrderedMap, len(rowsA))
		for idx, row := range rowsA {
			key := buildRowKey(row, summary.PrimaryKey, idx)
			rowMapA[key] = row
		}

		rowMapB := make(map[string]types.OrderedMap, len(rowsB))
		for idx, row := range rowsB {
			key := buildRowKey(row, summary.PrimaryKey, idx)
			rowMapB[key] = row
		}

		var commonKeys []string
		var missingInB []string
		for key := range rowMapA {
			if _, ok := rowMapB[key]; ok {
				commonKeys = append(commonKeys, key)
			} else {
				missingInB = append(missingInB, key)
			}
		}

		var missingInA []string
		for key := range rowMapB {
			if _, ok := rowMapA[key]; !ok {
				missingInA = append(missingInA, key)
			}
		}

		sort.Strings(commonKeys)
		sort.Strings(missingInA)
		sort.Strings(missingInB)

		valueDiffs := make([]row, 0, len(commonKeys))
		for _, key := range commonKeys {
			rowA := OrderedMapToMap(rowMapA[key])
			rowB := OrderedMapToMap(rowMapB[key])
			if !rowsDiffer(rowMapA[key], rowMapB[key], columns) {
				continue
			}

			var cells []cell
			for _, col := range columns {
				valA := stringifyCellValue(rowA[col])
				valB := stringifyCellValue(rowB[col])
				_, isPK := pkSet[col]

				htmlA, htmlB := highlightDifference(valA, valB)
				c := cell{
					Column:    col,
					IsKey:     isPK,
					NodeAHTML: htmlA,
					NodeBHTML: htmlB,
				}
				if valA != valB {
					c.NodeAClass = "value-diff"
					c.NodeBClass = "value-diff"
				}
				cells = append(cells, c)
			}
			valueDiffs = append(valueDiffs, row{Cells: cells})
		}

		missingGroups := make([]missingGroup, 0)
		if len(missingInB) > 0 {
			group := missingGroup{Title: fmt.Sprintf("Missing in %s", nodeB)}
			for _, key := range missingInB {
				rowA := OrderedMapToMap(rowMapA[key])
				var cells []cell
				for _, col := range columns {
					valA := stringifyCellValue(rowA[col])
					_, isPK := pkSet[col]
					cells = append(cells, cell{
						Column:     col,
						IsKey:      isPK,
						NodeAHTML:  plainHTML(valA),
						NodeBHTML:  plainHTML("MISSING"),
						NodeBClass: "missing",
					})
				}
				group.Rows = append(group.Rows, row{Cells: cells})
			}
			missingGroups = append(missingGroups, group)
		}

		if len(missingInA) > 0 {
			group := missingGroup{Title: fmt.Sprintf("Missing in %s", nodeA)}
			for _, key := range missingInA {
				rowB := OrderedMapToMap(rowMapB[key])
				var cells []cell
				for _, col := range columns {
					valB := stringifyCellValue(rowB[col])
					_, isPK := pkSet[col]
					cells = append(cells, cell{
						Column:     col,
						IsKey:      isPK,
						NodeAHTML:  plainHTML("MISSING"),
						NodeAClass: "missing",
						NodeBHTML:  plainHTML(valB),
					})
				}
				group.Rows = append(group.Rows, row{Cells: cells})
			}
			if len(missingGroups) > 0 {
				group.DividerBefore = true
			}
			missingGroups = append(missingGroups, group)
		}

		pair := pairSection{
			NodeA:      nodeA,
			NodeB:      nodeB,
			DiffCount:  formatInt64WithCommas(int64(summary.DiffRowsCount[pairKey])),
			ValueDiffs: valueDiffs,
			Missing:    missingGroups,
			HasDiffs:   len(valueDiffs) > 0 || len(missingGroups) > 0,
		}
		pairs = append(pairs, pair)
	}

	report := reportData{
		Summary: summaryData{
			Items:     filteredItems,
			Breakdown: buildDiffBreakdown(summary.DiffRowsCount),
		},
		Pairs: pairs,
	}

	tmpl, err := template.New("tableDiffReport").Parse(htmlDiffTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse HTML template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, report); err != nil {
		return "", fmt.Errorf("failed to render HTML diff report: %w", err)
	}

	if err := os.WriteFile(htmlPath, buf.Bytes(), 0644); err != nil {
		return "", fmt.Errorf("failed to write HTML diff report: %w", err)
	}

	return htmlPath, nil
}

func highlightDifference(a, b string) (template.HTML, template.HTML) {
	if a == b {
		esc := template.HTMLEscapeString(a)
		return template.HTML(esc), template.HTML(esc)
	}

	runesA := []rune(a)
	runesB := []rune(b)

	prefix := 0
	maxPrefix := len(runesA)
	if len(runesB) < maxPrefix {
		maxPrefix = len(runesB)
	}
	for prefix < maxPrefix && runesA[prefix] == runesB[prefix] {
		prefix++
	}

	suffix := 0
	for suffix < len(runesA)-prefix && suffix < len(runesB)-prefix && runesA[len(runesA)-suffix-1] == runesB[len(runesB)-suffix-1] {
		suffix++
	}

	prefixA, suffixA := prefix, suffix
	prefixB, suffixB := prefix, suffix

	if len(runesA)-prefixA-suffixA <= 0 {
		prefixA = 0
		suffixA = 0
	}
	if len(runesB)-prefixB-suffixB <= 0 {
		prefixB = 0
		suffixB = 0
	}

	highlightedA := renderHighlighted(runesA, prefixA, suffixA)
	highlightedB := renderHighlighted(runesB, prefixB, suffixB)
	return highlightedA, highlightedB
}

func renderHighlighted(value []rune, prefix, suffix int) template.HTML {
	var builder strings.Builder

	if prefix > 0 {
		builder.WriteString(template.HTMLEscapeString(string(value[:prefix])))
	}

	middleLen := len(value) - prefix - suffix
	if middleLen > 0 {
		builder.WriteString(`<span class="diff-chunk">`)
		builder.WriteString(template.HTMLEscapeString(string(value[prefix : prefix+middleLen])))
		builder.WriteString(`</span>`)
	}

	if suffix > 0 {
		builder.WriteString(template.HTMLEscapeString(string(value[len(value)-suffix:])))
	}

	return template.HTML(builder.String())
}

func plainHTML(value string) template.HTML {
	return template.HTML(template.HTMLEscapeString(value))
}

const htmlDiffTemplate = `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>ACE Table Diff Report</title>
    <style>
        :root {
            --primary-color: #1f6feb;
            --success-color: #1a7f37;
            --warning-color: #bf8700;
            --danger-color: #d73a49;
            --text-primary: #1b1f23;
            --text-secondary: #57606a;
            --bg-primary: #ffffff;
            --bg-secondary: #f6f8fa;
            --border-color: #d0d7de;
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            line-height: 1.5;
            color: var(--text-primary);
            background-color: var(--bg-secondary);
        }

        .container {
            max-width: 1400px;
            margin: 2rem auto;
            padding: 0 1rem 3rem;
        }

        h1, h2 {
            color: var(--text-primary);
            font-weight: 600;
            margin-bottom: 1rem;
        }

        h1 {
            font-size: 2rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border-color);
        }

        .summary-box {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1.5rem;
            margin: 1.5rem 0;
            box-shadow: 0 6px 16px rgba(27, 31, 35, 0.07);
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.5rem;
        }

        .summary-item {
            padding: 1rem;
            background: var(--bg-secondary);
            border-radius: 6px;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }

        .summary-item:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 18px rgba(27, 31, 35, 0.12);
        }

        .summary-label {
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-bottom: 0.35rem;
            letter-spacing: 0.02em;
        }

        .summary-value {
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            word-break: break-word;
        }

        .summary-breakdown {
            margin-top: 1.5rem;
            padding: 1rem;
            border: 1px dashed var(--border-color);
            border-radius: 6px;
            background: rgba(31, 111, 235, 0.05);
            color: var(--text-secondary);
        }

        .summary-breakdown-title {
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-primary);
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        .summary-breakdown-items {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
        }

        .breakdown-item {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 4px;
            padding: 0.35rem 0.75rem;
            font-size: 0.9rem;
            display: flex;
            align-items: baseline;
            gap: 0.5rem;
            box-shadow: 0 3px 6px rgba(27, 31, 35, 0.06);
        }

        .breakdown-name {
            font-weight: 600;
            color: var(--primary-color);
        }

        .breakdown-count {
            font-variant-numeric: tabular-nums;
            color: var(--text-secondary);
        }

        .diff-section {
            background: var(--bg-primary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            margin: 1.5rem 0;
            box-shadow: 0 6px 16px rgba(27, 31, 35, 0.07);
            overflow: hidden;
        }

        .diff-header {
            background: var(--primary-color);
            color: white;
            padding: 1rem 1.5rem;
            font-size: 1.1rem;
            font-weight: 550;
        }

        .table-wrapper {
            width: 100%;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-size: 0.92rem;
            min-width: 640px;
        }

        th {
            background: var(--bg-secondary);
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            border-bottom: 2px solid var(--border-color);
            position: sticky;
            top: 0;
            z-index: 5;
        }

        td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border-color);
            vertical-align: top;
            background: var(--bg-primary);
            white-space: pre-wrap;
            word-break: break-word;
        }

        tr:hover td {
            background-color: rgba(31, 111, 235, 0.04);
        }

        .value-diff {
            background-color: rgba(215, 58, 73, 0.08);
        }

        .diff-chunk {
            background-color: rgba(215, 58, 73, 0.35);
            color: var(--danger-color);
            border-radius: 3px;
            padding: 0 2px;
        }

        .key-column {
            font-weight: 600;
            color: var(--primary-color);
        }

        .missing {
            background-color: rgba(191, 135, 0, 0.12);
            color: var(--warning-color);
            font-style: italic;
            font-weight: 600;
        }

        .row-separator {
            background: rgba(31, 111, 235, 0.08);
            color: var(--primary-color);
            font-weight: 600;
            text-align: center;
            padding: 0.75rem;
            letter-spacing: 0.04em;
        }

        .row-subseparator {
            background: rgba(31, 111, 235, 0.04);
            color: var(--text-secondary);
            font-weight: 500;
            text-align: center;
            padding: 0.65rem;
        }

        .row-divider td {
            background: var(--bg-secondary);
            border-bottom: none;
            height: 0.4rem;
            padding: 0;
        }

        .empty-message {
            padding: 1.5rem;
            color: var(--text-secondary);
            text-align: center;
        }

        @media (max-width: 768px) {
            .container {
                padding: 0 0.5rem 2rem;
            }

            table {
                min-width: 100%;
            }

            td, th {
                padding: 0.65rem;
            }

            .summary-breakdown-items {
                flex-direction: column;
                gap: 0.75rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>ACE Table Diff Report</h1>
        <div class="summary-box">
            <h2>Summary</h2>
            <div class="summary-grid">
                {{range .Summary.Items}}
                <div class="summary-item">
                    <div class="summary-label">{{.Label}}</div>
                    <div class="summary-value">{{.Value}}</div>
                </div>
                {{end}}
            </div>
            {{if .Summary.Breakdown}}
            <div class="summary-breakdown">
                <div class="summary-breakdown-title">Diffs by node pair</div>
                <div class="summary-breakdown-items">
                    {{range .Summary.Breakdown}}
                    <div class="breakdown-item">
                        <span class="breakdown-name">{{.Name}}</span>
                        <span class="breakdown-count">{{.Count}}</span>
                    </div>
                    {{end}}
                </div>
            </div>
            {{end}}
        </div>

        {{if .Pairs}}
            {{range .Pairs}}
            <div class="diff-section">
                <div class="diff-header">Differences between {{.NodeA}} and {{.NodeB}} ({{.DiffCount}} entries)</div>
                <div class="table-wrapper">
                    <table>
                        <tr>
                            <th>Column</th>
                            <th>{{.NodeA}}</th>
                            <th>{{.NodeB}}</th>
                        </tr>
                        {{if .ValueDiffs}}
                        <tr><td colspan="3" class="row-separator">Value Differences</td></tr>
                        {{range $i, $row := .ValueDiffs}}
                            {{if $i}}<tr class="row-divider"><td colspan="3"></td></tr>{{end}}
                            {{range $row.Cells}}
                            <tr>
                                <td{{if .IsKey}} class="key-column"{{end}}>{{.Column}}</td>
                                <td{{if .NodeAClass}} class="{{.NodeAClass}}"{{end}}>{{.NodeAHTML}}</td>
                                <td{{if .NodeBClass}} class="{{.NodeBClass}}"{{end}}>{{.NodeBHTML}}</td>
                            </tr>
                            {{end}}
                        {{end}}
                        {{end}}

                        {{if .Missing}}
                        <tr><td colspan="3" class="row-separator">Missing Rows</td></tr>
                        {{range $gIndex, $group := .Missing}}
                            {{if $group.DividerBefore}}<tr class="row-divider"><td colspan="3"></td></tr>{{end}}
                            <tr><td colspan="3" class="row-subseparator">{{$group.Title}}</td></tr>
                            {{range $rIndex, $row := $group.Rows}}
                                {{if $rIndex}}<tr class="row-divider"><td colspan="3"></td></tr>{{end}}
                                {{range $row.Cells}}
                                <tr>
                                    <td{{if .IsKey}} class="key-column"{{end}}>{{.Column}}</td>
                                    <td{{if .NodeAClass}} class="{{.NodeAClass}}"{{end}}>{{.NodeAHTML}}</td>
                                    <td{{if .NodeBClass}} class="{{.NodeBClass}}"{{end}}>{{.NodeBHTML}}</td>
                                </tr>
                                {{end}}
                            {{end}}
                        {{end}}
                        {{end}}

                        {{if not .HasDiffs}}
                        <tr><td colspan="3" class="empty-message">No column level differences detected for this node pair.</td></tr>
                        {{end}}
                    </table>
                </div>
            </div>
            {{end}}
        {{else}}
            <div class="diff-section">
                <div class="empty-message">No row-level differences were recorded.</div>
            </div>
        {{end}}
    </div>
</body>
</html>`

func totalDiffs(diffCounts map[string]int) int64 {
	var total int64
	for _, count := range diffCounts {
		total += int64(count)
	}
	return total
}

func buildDiffBreakdown(diffCounts map[string]int) []htmlPairCount {
	if len(diffCounts) == 0 {
		return nil
	}

	pairs := make([]htmlPairCount, 0, len(diffCounts))
	for pair, count := range diffCounts {
		pairs = append(pairs, htmlPairCount{Name: pair, Count: formatInt64WithCommas(int64(count))})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}

func buildRowKey(row types.OrderedMap, primaryKey []string, index int) string {
	if len(primaryKey) == 0 {
		return fmt.Sprintf("__row_%d", index)
	}

	key, err := StringifyOrderedMapKey(row, primaryKey)
	if err != nil || key == "" {
		return fmt.Sprintf("__row_%d", index)
	}
	return key
}

func collectColumnsInOrder(primaryKey []string, rowSets ...[]types.OrderedMap) []string {
	seen := make(map[string]struct{})
	var columns []string

	for _, rows := range rowSets {
		for _, row := range rows {
			for _, kv := range row {
				if kv.Key == "_spock_metadata_" {
					continue
				}
				if _, ok := seen[kv.Key]; !ok {
					seen[kv.Key] = struct{}{}
					columns = append(columns, kv.Key)
				}
			}
		}
	}

	return reorderColumnsWithPriority(columns, primaryKey)
}

func reorderColumnsWithPriority(columns, priority []string) []string {
	ordered := make([]string, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))

	for _, col := range priority {
		for _, candidate := range columns {
			if candidate == col {
				if _, ok := seen[candidate]; !ok {
					ordered = append(ordered, candidate)
					seen[candidate] = struct{}{}
				}
				break
			}
		}
	}

	for _, col := range columns {
		if _, ok := seen[col]; ok {
			continue
		}
		ordered = append(ordered, col)
		seen[col] = struct{}{}
	}

	return ordered
}

func rowsDiffer(rowA, rowB types.OrderedMap, columns []string) bool {
	mapA := OrderedMapToMap(rowA)
	mapB := OrderedMapToMap(rowB)

	for _, col := range columns {
		valA, okA := mapA[col]
		valB, okB := mapB[col]

		if !okA && !okB {
			continue
		}
		if stringifyCellValue(valA) != stringifyCellValue(valB) {
			return true
		}
	}

	return false
}

func stringifyCellValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case json.Number:
		return v.String()
	default:
		if v == nil {
			return "NULL"
		}
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

func formatInt64WithCommas(value int64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	s := strconv.FormatInt(value, 10)
	n := len(s)
	if n <= 3 {
		return sign + s
	}

	var builder strings.Builder
	builder.Grow(len(s) + len(s)/3)

	remainder := n % 3
	if remainder == 0 {
		remainder = 3
	}
	builder.WriteString(s[:remainder])
	for i := remainder; i < n; i += 3 {
		builder.WriteString(",")
		builder.WriteString(s[i : i+3])
	}

	return sign + builder.String()
}

func formatDurationHuman(durationStr string) string {
	if durationStr == "" {
		return ""
	}

	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		return durationStr
	}

	if dur < time.Millisecond {
		return fmt.Sprintf("%dµs", dur/time.Microsecond)
	}
	if dur < time.Second {
		return fmt.Sprintf("%.2f ms", float64(dur)/float64(time.Millisecond))
	}
	if dur < time.Minute {
		return fmt.Sprintf("%.2f s", dur.Seconds())
	}
	minutes := int(dur.Minutes())
	seconds := int(dur.Seconds()) % 60
	if dur < time.Hour {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}

func formatTimestampHuman(ts string) string {
	if ts == "" {
		return ""
	}

	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ts
	}
	return t.Format("02 Jan 2006 15:04:05 MST")
}

func formatPrimaryKey(pk []string) string {
	if len(pk) == 0 {
		return "N/A"
	}
	return strings.Join(pk, ", ")
}
