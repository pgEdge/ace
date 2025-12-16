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

	rawJSON, err := json.Marshal(diffResult)
	if err != nil {
		return "", fmt.Errorf("failed to marshal diff result for HTML embedding: %w", err)
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
		HasDiff    bool
	}

	type row struct {
		PKey     string
		Cells    []cell
		RowType  string // "value_diff", "missing_in_a", "missing_in_b"
		HasDiffs bool
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
		Summary     summaryData
		Pairs       []pairSection
		RawDiffJSON template.JS
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
			hasDiffs := false
			for _, col := range columns {
				valA := stringifyCellValue(rowA[col])
				valB := stringifyCellValue(rowB[col])
				_, isPK := pkSet[col]

				htmlA, htmlB := highlightDifference(valA, valB)
				hasDiff := valA != valB
				c := cell{
					Column:    col,
					IsKey:     isPK,
					NodeAHTML: htmlA,
					NodeBHTML: htmlB,
					HasDiff:   hasDiff,
				}
				if hasDiff {
					c.NodeAClass = "value-diff"
					c.NodeBClass = "value-diff"
					hasDiffs = true
				}
				cells = append(cells, c)
			}
			valueDiffs = append(valueDiffs, row{
				PKey:     key,
				Cells:    cells,
				RowType:  "value_diff",
				HasDiffs: hasDiffs,
			})
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
						HasDiff:    false,
					})
				}
				group.Rows = append(group.Rows, row{
					PKey:     key,
					Cells:    cells,
					RowType:  "missing_in_b",
					HasDiffs: true,
				})
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
						HasDiff:    false,
					})
				}
				group.Rows = append(group.Rows, row{
					PKey:     key,
					Cells:    cells,
					RowType:  "missing_in_a",
					HasDiffs: true,
				})
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
		Pairs:       pairs,
		RawDiffJSON: template.JS(rawJSON),
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
            --primary-strong: #1557b0;
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
            background: linear-gradient(180deg, #f4f7fb 0%, #eef1f7 100%);
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

        .plan-builder {
            margin: 1.5rem 0;
            padding: 1rem 1.25rem;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            background: #eef6ff;
        }

        .plan-builder h3 {
            margin-bottom: 0.5rem;
        }

        .plan-builder button {
            background: var(--primary-color);
            color: #fff;
            border: none;
            padding: 0.6rem 1rem;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 600;
        }

        .plan-builder button:hover {
            background: #1557b0;
        }

        .plan-builder .note {
            margin-top: 0.35rem;
            color: var(--text-secondary);
            font-size: 0.9rem;
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
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            background: linear-gradient(120deg, #1f6feb 0%, #1b5dcc 100%);
            color: white;
            padding: 1rem 1.5rem;
            font-size: 1.05rem;
            font-weight: 650;
        }

        .pair-title {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            align-items: center;
        }

        .diff-count-pill {
            background: rgba(255, 255, 255, 0.14);
            border: 1px solid rgba(255, 255, 255, 0.3);
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            font-variant-numeric: tabular-nums;
            font-size: 0.9rem;
            color: #f6f8fa;
            box-shadow: inset 0 0 0 1px rgba(0, 0, 0, 0.03);
        }

        .table-wrapper {
            width: 100%;
            overflow-x: auto;
            background: var(--bg-secondary);
            border-top: 1px solid var(--border-color);
        }

        .diff-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.92rem;
        }

        .diff-table .action-cell,
        .diff-table .action-header {
            width: 220px;
            min-width: 220px;
            max-width: 280px;
        }

        .diff-table thead th {
            background: var(--bg-secondary);
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 650;
            color: var(--text-secondary);
            border-bottom: 2px solid var(--border-color);
            position: sticky;
            top: 0;
            z-index: 5;
        }

        .inner-columns-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: auto;
        }

        .diff-row {
            border-bottom: 1px solid var(--border-color);
        }

        .diff-row:hover {
            background-color: rgba(31, 111, 235, 0.02);
        }

        .diff-row .action-cell {
            background: #eef3ff;
            border-right: 2px solid var(--border-color);
            vertical-align: top;
            padding: 1rem;
        }

        .diff-row .columns-cell {
            padding: 0;
            vertical-align: top;
        }

        .action-wrapper {
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .action-label {
            display: block;
            font-size: 0.75rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: var(--text-secondary);
            font-weight: 600;
        }

        .plan-action {
            width: 100%;
            padding: 0.5rem;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            background: var(--bg-primary);
            font-size: 0.85rem;
            cursor: pointer;
        }

        .plan-action:hover {
            border-color: var(--primary-color);
        }

        .row-key {
            padding: 0.4rem 0.6rem;
            background: rgba(31, 111, 235, 0.08);
            border-radius: 4px;
            color: var(--text-primary);
            font-size: 0.8rem;
            font-family: 'Courier New', monospace;
            word-break: break-all;
            font-weight: 500;
        }

        .inner-columns-table thead {
            position: sticky;
            top: 0;
            z-index: 2;
        }

        .inner-columns-table th {
            background: #e8ecf3;
            padding: 0.6rem 1rem;
            text-align: left;
            font-weight: 600;
            font-size: 0.85rem;
            color: var(--text-primary);
            border-bottom: 2px solid var(--border-color);
            border-right: 1px solid #d8dde5;
        }

        .inner-columns-table th:last-child {
            border-right: none;
        }

        .inner-columns-table .inner-col-header {
            width: 150px;
            min-width: 150px;
            max-width: 200px;
            color: var(--text-secondary);
        }

        .inner-columns-table .inner-node-header {
            min-width: 250px;
            text-align: left;
        }

        .inner-columns-table tbody tr {
            border-bottom: 1px solid #e8ecf0;
        }

        .inner-columns-table tbody tr:last-child {
            border-bottom: none;
        }

        .inner-columns-table tbody tr.has-diff {
            background: rgba(255, 247, 230, 0.5);
        }

        .inner-columns-table td {
            padding: 0.75rem 1rem;
            vertical-align: top;
            border-right: 1px solid #e8ecf0;
        }

        .inner-columns-table td:last-child {
            border-right: none;
        }

        .col-name {
            font-weight: 600;
            color: var(--text-secondary);
            background: #f8f9fb;
            white-space: nowrap;
            width: 150px;
            min-width: 150px;
            max-width: 200px;
        }

        .col-name.key-column {
            background: #e3f2fd;
            color: var(--primary-color);
            font-weight: 700;
        }

        .col-name.key-column::after {
            content: " 🔑";
            font-size: 0.75em;
            opacity: 0.7;
        }

        .value-cell {
            background: var(--bg-primary);
            white-space: pre-wrap;
            word-break: break-word;
            min-width: 200px;
        }

        .value-diff {
            background-color: rgba(255, 235, 238, 0.6);
            border-left: 3px solid var(--danger-color);
            position: relative;
        }

        .value-diff::before {
            content: "";
            position: absolute;
            left: -3px;
            top: 0;
            bottom: 0;
            width: 3px;
            background: var(--danger-color);
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

        .row-separator td {
            background: rgba(31, 111, 235, 0.1);
            color: var(--primary-color);
            font-weight: 650;
            text-align: center;
            padding: 0.85rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            border-bottom: 2px solid var(--primary-color);
            font-size: 0.9rem;
        }

        .row-subseparator td {
            background: rgba(31, 111, 235, 0.05);
            color: var(--text-primary);
            font-weight: 600;
            text-align: center;
            padding: 0.7rem;
            letter-spacing: 0.02em;
            border-bottom: 1px solid var(--border-color);
            font-size: 0.88rem;
        }

        .row-divider td {
            background: var(--bg-secondary);
            border-bottom: none;
            height: 0.75rem;
            padding: 0;
        }

        .empty-message {
            padding: 1.5rem;
            color: var(--text-secondary);
            text-align: center;
            background: var(--bg-secondary);
            border: 1px dashed var(--border-color);
        }

        .bulk-bar {
            display: flex;
            align-items: center;
            gap: 0.65rem;
            padding: 0.9rem 1.5rem;
            background: #f7f9ff;
            border-bottom: 1px solid var(--border-color);
        }

        .bulk-label {
            font-weight: 600;
            color: var(--text-primary);
            white-space: nowrap;
        }

        .bulk-select {
            min-width: 200px;
            padding: 0.45rem 0.5rem;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            background: var(--bg-primary);
        }

        .bulk-apply {
            padding: 0.45rem 0.85rem;
            border: 1px solid var(--primary-strong);
            background: var(--primary-color);
            color: #fff;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            box-shadow: 0 6px 16px rgba(27, 31, 35, 0.12);
        }

        .bulk-apply:hover {
            background: var(--primary-strong);
        }

        @media (max-width: 768px) {
            .container {
                padding: 0 0.5rem 2rem;
            }

            h1 {
                font-size: 1.5rem;
            }

            .diff-table .action-cell,
            .diff-table .action-header {
                width: 180px;
                min-width: 180px;
            }

            .inner-columns-table th,
            .inner-columns-table td {
                padding: 0.5rem 0.75rem;
                font-size: 0.85rem;
            }

            .inner-columns-table .inner-col-header,
            .col-name {
                width: 100px;
                min-width: 100px;
                max-width: 120px;
            }

            .value-cell {
                min-width: 150px;
            }

            .action-cell {
                padding: 0.75rem;
            }

            .plan-action {
                font-size: 0.8rem;
                padding: 0.4rem;
            }

            .row-key {
                font-size: 0.75rem;
                padding: 0.3rem 0.5rem;
            }

            .summary-breakdown-items {
                flex-direction: column;
                gap: 0.75rem;
            }

            .bulk-bar {
                flex-direction: column;
                align-items: stretch;
                gap: 0.5rem;
            }

            .bulk-select {
                min-width: 100%;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>ACE Table Diff Report</h1>
        <div class="plan-builder">
            <h3>Build an advanced repair plan</h3>
            <p>Create a starter repair file (YAML) from this diff. The plan will set <code>keep_n1</code> for mismatches, <code>apply_from n1 insert</code> for rows missing on node2, and <code>apply_from n2 insert</code> for rows missing on node1. You can edit the file to add rules or change actions.</p>
            <button id="download-plan">Download repair plan</button>
            <div class="note">Generated plan uses per-row overrides. For large diffs, convert repeating patterns into rules manually.</div>
        </div>
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
            <div class="diff-section" data-nodea="{{.NodeA}}" data-nodeb="{{.NodeB}}">
                {{ $nodeA := .NodeA }}{{ $nodeB := .NodeB }}
                <div class="diff-header">
                    <div class="pair-title">Differences between {{.NodeA}} and {{.NodeB}}</div>
                    <span class="diff-count-pill">{{.DiffCount}} entries</span>
                </div>
                <div class="table-wrapper">
                    <table class="diff-table">
                        <thead>
                            <tr>
                                <th class="action-header">Action</th>
                                <th class="data-header">Row Data</th>
                            </tr>
                        </thead>
                        <tbody>
                        {{if .ValueDiffs}}
                        <tr class="row-separator"><td colspan="100%">Value Differences</td></tr>
                        {{range $i, $row := .ValueDiffs}}
                            {{if $i}}<tr class="row-divider"><td colspan="100%"></td></tr>{{end}}
                            <tr class="diff-row">
                                <td class="action-cell">
                                    <div class="action-wrapper">
                                        <span class="action-label">Row Action</span>
                                        <select class="plan-action" data-type="mismatch" data-pk="{{$row.PKey}}" data-nodea="{{ $nodeA }}" data-nodeb="{{ $nodeB }}">
                                            <option value="">Use default (keep_n1)</option>
                                            <option value="keep_n1">Keep {{$nodeA}}</option>
                                            <option value="keep_n2">Keep {{$nodeB}}</option>
                                            <option value="apply_from_n1_insert">Apply from {{$nodeA}} (insert if missing)</option>
                                            <option value="apply_from_n2_insert">Apply from {{$nodeB}} (insert if missing)</option>
                                            <option value="delete">Delete</option>
                                            <option value="skip">Skip</option>
                                        </select>
                                        <div class="row-key" title="Primary key">{{$row.PKey}}</div>
                                    </div>
                                </td>
                                <td class="columns-cell">
                                    <table class="inner-columns-table">
                                        <thead>
                                            <tr>
                                                <th class="inner-col-header">Column</th>
                                                <th class="inner-node-header">{{ $nodeA }}</th>
                                                <th class="inner-node-header">{{ $nodeB }}</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {{range $row.Cells}}
                                            <tr{{if .HasDiff}} class="has-diff"{{end}}>
                                                <td class="col-name{{if .IsKey}} key-column{{end}}">{{.Column}}</td>
                                                <td class="value-cell{{if .NodeAClass}} {{.NodeAClass}}{{end}}">{{.NodeAHTML}}</td>
                                                <td class="value-cell{{if .NodeBClass}} {{.NodeBClass}}{{end}}">{{.NodeBHTML}}</td>
                                            </tr>
                                            {{end}}
                                        </tbody>
                                    </table>
                                </td>
                            </tr>
                        {{end}}
                        {{end}}

                        {{if .Missing}}
                        <tr class="row-separator"><td colspan="100%">Missing Rows</td></tr>
                        {{range $gIndex, $group := .Missing}}
                            {{if $group.DividerBefore}}<tr class="row-divider"><td colspan="100%"></td></tr>{{end}}
                            <tr class="row-subseparator"><td colspan="100%">{{$group.Title}}</td></tr>
                            {{range $rIndex, $row := $group.Rows}}
                                {{if $rIndex}}<tr class="row-divider"><td colspan="100%"></td></tr>{{end}}
                                <tr class="diff-row">
                                    <td class="action-cell">
                                        <div class="action-wrapper">
                                            <span class="action-label">Row Action</span>
                                            <select class="plan-action" data-type="{{if eq $group.Title (print "Missing in " $nodeB)}}missing_in_b{{else}}missing_in_a{{end}}" data-pk="{{$row.PKey}}" data-nodea="{{ $nodeA }}" data-nodeb="{{ $nodeB }}">
                                                <option value="">Use default</option>
                                                <option value="apply_from_n1_insert">Insert from {{$nodeA}}</option>
                                                <option value="apply_from_n2_insert">Insert from {{$nodeB}}</option>
                                                <option value="delete">Delete</option>
                                                <option value="skip">Skip</option>
                                            </select>
                                            <div class="row-key" title="Primary key">{{$row.PKey}}</div>
                                        </div>
                                    </td>
                                    <td class="columns-cell">
                                        <table class="inner-columns-table">
                                            <thead>
                                                <tr>
                                                    <th class="inner-col-header">Column</th>
                                                    <th class="inner-node-header">{{ $nodeA }}</th>
                                                    <th class="inner-node-header">{{ $nodeB }}</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {{range $row.Cells}}
                                                <tr>
                                                    <td class="col-name{{if .IsKey}} key-column{{end}}">{{.Column}}</td>
                                                    <td class="value-cell{{if .NodeAClass}} {{.NodeAClass}}{{end}}">{{.NodeAHTML}}</td>
                                                    <td class="value-cell{{if .NodeBClass}} {{.NodeBClass}}{{end}}">{{.NodeBHTML}}</td>
                                                </tr>
                                                {{end}}
                                            </tbody>
                                        </table>
                                    </td>
                                </tr>
                            {{end}}
                        {{end}}
                        {{end}}

                        {{if not .HasDiffs}}
                        <tr><td colspan="2" class="empty-message">No column level differences detected for this node pair.</td></tr>
                        {{end}}
                        </tbody>
                    </table>
                </div>
            </div>
            {{end}}
        {{else}}
            <div class="diff-section">
                <div class="empty-message">No row-level differences were recorded.</div>
            </div>
        {{end}}
        <script id="diff-data" type="application/json">{{ .RawDiffJSON }}</script>
        <script>
            (function () {
                const diffDataEl = document.getElementById('diff-data');
                const downloadBtn = document.getElementById('download-plan');
                const sections = document.querySelectorAll('.diff-section');
                if (!diffDataEl || !downloadBtn) return;

                const diff = JSON.parse(diffDataEl.textContent);

                sections.forEach(section => {
                    const controls = section.querySelectorAll('.plan-action');
                    if (controls.length === 0) return;
                    const nodeALabel = section.dataset.nodea || 'node A';
                    const nodeBLabel = section.dataset.nodeb || 'node B';
                    const bulkBar = document.createElement('div');
                    bulkBar.className = 'bulk-bar';
                    const label = document.createElement('span');
                    label.className = 'bulk-label';
                    label.textContent = 'Apply to all for ' + nodeALabel + ' vs ' + nodeBLabel + ':';
                    const select = document.createElement('select');
                    select.className = 'bulk-select';
                    select.setAttribute('aria-label', 'Bulk action for ' + nodeALabel + ' and ' + nodeBLabel);
                    [
                        { value: '', label: 'Choose action' },
                        { value: 'keep_n1', label: 'Keep ' + nodeALabel },
                        { value: 'keep_n2', label: 'Keep ' + nodeBLabel },
                        { value: 'apply_from_n1_insert', label: 'Insert from ' + nodeALabel },
                        { value: 'apply_from_n2_insert', label: 'Insert from ' + nodeBLabel },
                        { value: 'delete', label: 'Delete' },
                        { value: 'skip', label: 'Skip' }
                    ].forEach(opt => {
                        const option = document.createElement('option');
                        option.value = opt.value;
                        option.textContent = opt.label;
                        select.appendChild(option);
                    });
                    const applyBtn = document.createElement('button');
                    applyBtn.type = 'button';
                    applyBtn.className = 'bulk-apply';
                    applyBtn.textContent = 'Apply';
                    applyBtn.addEventListener('click', () => {
                        const val = select.value;
                        if (!val) return;
                        controls.forEach(sel => sel.value = val);
                    });
                    bulkBar.appendChild(label);
                    bulkBar.appendChild(select);
                    bulkBar.appendChild(applyBtn);
                    section.insertBefore(bulkBar, section.children[1]);
                });

                downloadBtn.addEventListener('click', () => {
                    try {
                        const yaml = buildPlanYaml(diff);
                        const blob = new Blob([yaml], { type: 'text/yaml' });
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        const tableKey = diff.summary.schema + '.' + diff.summary.table;
                        a.download = tableKey.replace('.', '_') + '_repair_plan.yaml';
                        document.body.appendChild(a);
                        a.click();
                        document.body.removeChild(a);
                        URL.revokeObjectURL(url);
                    } catch (e) {
                        alert('Failed to build repair plan: ' + e);
                    }
                });

                function buildPlanYaml(diff) {
                    const pkCols = diff.summary.primary_key || [];
                    if (!pkCols.length) throw new Error('No primary key info available');
                    const tableKey = diff.summary.schema + '.' + diff.summary.table;

                    const overrides = [];
                    const seen = new Set();

                    const nodeDiffs = diff.NodeDiffs || diff.diffs || {};
                    for (const pairKey of Object.keys(nodeDiffs)) {
                        const nodeDiff = nodeDiffs[pairKey];
                        if (!nodeDiff || !nodeDiff.Rows && !nodeDiff.rows) continue;
                        const rowsMap = nodeDiff.Rows || nodeDiff.rows;
                        const nodeNames = Object.keys(rowsMap || {}).sort();
                        if (nodeNames.length < 2) continue;
                        const n1 = nodeNames[0];
                        const n2 = nodeNames[1];
                        const rows1 = rowsMap[n1] || [];
                        const rows2 = rowsMap[n2] || [];

                        const map1 = rowsToMap(rows1, pkCols);
                        const map2 = rowsToMap(rows2, pkCols);

                        const keys = new Set([...map1.keys(), ...map2.keys()]);
                        for (const key of keys) {
                            if (seen.has(key)) continue;
                            seen.add(key);
                            const row1 = map1.get(key);
                            const row2 = map2.get(key);
                            const pkMap = keyToPkMap(key, pkCols);

                            let action = selectionForKey(key) || { type: 'keep_n1' };
                            let name = 'pk_' + key.replace(/\|/g, '_');
                            if (row1 && !row2) {
                                action = selectionForKey(key) || { type: 'apply_from', from: n1, mode: 'insert' };
                            } else if (row2 && !row1) {
                                action = selectionForKey(key) || { type: 'apply_from', from: n2, mode: 'insert' };
                            }

                            overrides.push({ name, pk: pkMap, action });
                        }
                    }

                    const lines = [];
                    lines.push('version: 1');
                    lines.push('tables:');
                    lines.push('  ' + tableKey + ':');
                    lines.push('    default_action:');
                    lines.push('      type: keep_n1');
                    if (overrides.length) {
                        lines.push('    row_overrides:');
                        for (const ov of overrides) {
                            lines.push('      - name: ' + quote(ov.name));
                            lines.push('        pk:');
                            for (const col of pkCols) {
                                const v = ov.pk[col];
                                lines.push('          ' + col + ': ' + scalar(v));
                            }
                            lines.push('        action:');
                            lines.push('          type: ' + ov.action.type);
                            if (ov.action.from) lines.push('          from: ' + ov.action.from);
                            if (ov.action.mode) lines.push('          mode: ' + ov.action.mode);
                        }
                    }
                    return lines.join('\n') + '\n';
                }

                function selectionForKey(key) {
                    const sel = document.querySelector('.plan-action[data-pk="' + key + '"]');
                    if (!sel) return null;
                    const val = sel.value;
                    if (!val) return null;
                    switch (val) {
                        case 'keep_n1':
                            return { type: 'keep_n1' };
                        case 'keep_n2':
                            return { type: 'keep_n2' };
                        case 'apply_from_n1_insert':
                            return { type: 'apply_from', from: sel.dataset.nodea, mode: 'insert' };
                        case 'apply_from_n2_insert':
                            return { type: 'apply_from', from: sel.dataset.nodeb, mode: 'insert' };
                        case 'delete':
                            return { type: 'delete' };
                        case 'skip':
                            return { type: 'skip' };
                        default:
                            return null;
                    }
                }

                function rowsToMap(rows, pkCols) {
                    const m = new Map();
                    for (const r of rows) {
                        const keyParts = pkCols.map(c => stringify(r[c]));
                        const k = keyParts.join('|');
                        m.set(k, r);
                    }
                    return m;
                }

                function keyToPkMap(key, pkCols) {
                    const parts = key.split('|');
                    const pk = {};
                    pkCols.forEach((c, i) => pk[c] = parseMaybeNumber(parts[i]));
                    return pk;
                }

                function stringify(v) {
                    if (v === null || v === undefined) return '';
                    return '' + v;
                }

                function parseMaybeNumber(v) {
                    if (v === undefined || v === null) return v;
                    const n = Number(v);
                    if (!Number.isNaN(n) && v !== '') return n;
                    return v;
                }

                function quote(s) {
                    if (/^[A-Za-z0-9._-]+$/.test(s)) return s;
                    return JSON.stringify(s);
                }

                function scalar(v) {
                    if (v === null || v === undefined) return 'null';
                    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
                    const str = String(v);
                    if (/^[A-Za-z0-9._-]+$/.test(str)) return str;
                    return JSON.stringify(str);
                }
            })();
        </script>
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
