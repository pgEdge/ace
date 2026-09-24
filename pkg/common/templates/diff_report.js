(function () {
    const diffDataEl = document.getElementById('diff-data');
    const sections = document.querySelectorAll('.diff-section');
    if (!diffDataEl) return;

    const diff = JSON.parse(diffDataEl.textContent);

    sections.forEach(section => {
        const controls = Array.from(section.querySelectorAll('.plan-action'));
        const checkboxes = Array.from(section.querySelectorAll('.row-select'));
        if (controls.length === 0 && checkboxes.length === 0) return;
        const nodeALabel = section.dataset.nodea || 'node A';
        const nodeBLabel = section.dataset.nodeb || 'node B';
        const bulkBar = document.createElement('div');
        bulkBar.className = 'bulk-bar';
        const selectAllBtn = document.createElement('button');
        selectAllBtn.type = 'button';
        selectAllBtn.className = 'select-all-btn';
        selectAllBtn.textContent = 'Select all rows';
        selectAllBtn.addEventListener('click', () => {
            if (!checkboxes.length) return;
            const shouldSelectAll = !checkboxes.every(cb => cb.checked);
            checkboxes.forEach(cb => {
                cb.checked = shouldSelectAll;
                toggleRowSelection(cb);
            });
            updateSelectAllState();
        });
        const clearSelectionBtn = document.createElement('button');
        clearSelectionBtn.type = 'button';
        clearSelectionBtn.className = 'clear-selection-btn';
        clearSelectionBtn.textContent = 'Clear selection';
        clearSelectionBtn.addEventListener('click', () => {
            if (!checkboxes.length) return;
            let changed = false;
            checkboxes.forEach(cb => {
                if (cb.checked) {
                    cb.checked = false;
                    toggleRowSelection(cb);
                    changed = true;
                }
            });
            if (changed) updateSelectAllState();
        });
        const label = document.createElement('span');
        label.className = 'bulk-label';
        label.textContent = 'Apply to all rows for ' + nodeALabel + ' vs ' + nodeBLabel + ':';
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
            const selectedCbs = checkboxes.filter(cb => cb.checked);
            if (selectedCbs.length) {
                selectedCbs.forEach(cb => {
                    const row = cb.closest('.diff-row');
                    const sel = row ? row.querySelector('.plan-action') : null;
                    if (sel) sel.value = val;
                });
            } else {
                controls.forEach(sel => sel.value = val);
            }
        });
        const copyBtn = createCopyButton();
        const downloadBtn = createDownloadButton();
        controls.forEach(sel => {
            sel.addEventListener('change', () => toggleCustomEditor(sel));
            toggleCustomEditor(sel);
        });
        checkboxes.forEach(cb => {
            cb.addEventListener('change', () => {
                toggleRowSelection(cb);
                updateSelectAllState();
            });
            toggleRowSelection(cb);
        });
        updateSelectAllState();

        bulkBar.appendChild(label);
        bulkBar.appendChild(select);
        bulkBar.appendChild(applyBtn);
        bulkBar.appendChild(clearSelectionBtn);
        bulkBar.appendChild(selectAllBtn);
        bulkBar.appendChild(copyBtn);
        bulkBar.appendChild(downloadBtn);
        section.insertBefore(bulkBar, section.children[1]);

        function toggleRowSelection(cb) {
            const row = cb.closest('.diff-row');
            if (row) row.classList.toggle('is-selected', cb.checked);
        }

        function updateSelectAllState() {
            const selectedCount = checkboxes.filter(cb => cb.checked).length;
            updateApplyLabel(selectedCount);
            clearSelectionBtn.disabled = selectedCount === 0;
            if (!checkboxes.length) {
                selectAllBtn.disabled = true;
                selectAllBtn.textContent = 'No rows';
                clearSelectionBtn.disabled = true;
                return;
            }
            const allSelected = checkboxes.every(cb => cb.checked);
            selectAllBtn.textContent = allSelected ? 'Clear selection' : 'Select all rows';
            selectAllBtn.classList.toggle('is-active', allSelected);
            selectAllBtn.setAttribute('aria-pressed', allSelected ? 'true' : 'false');
        }

        function updateApplyLabel(selectedCount) {
            if (selectedCount > 0) {
                label.textContent = 'Apply to ' + selectedCount + ' selected ' + (selectedCount === 1 ? 'row' : 'rows') + ' for ' + nodeALabel + ' vs ' + nodeBLabel + ':';
            } else {
                label.textContent = 'Apply to all rows for ' + nodeALabel + ' vs ' + nodeBLabel + ':';
            }
        }

        function toggleCustomEditor(selectEl) {
            const wrapper = selectEl.closest('.action-wrapper');
            if (!wrapper) return;
            const editor = wrapper.querySelector('.custom-editor');
            if (!editor) return;
            const show = selectEl.value === 'custom';
            editor.classList.toggle('is-visible', show);
            if (show) {
                const textarea = editor.querySelector('.custom-row-input');
                if (textarea && !textarea.value.trim()) {
                    const def = textarea.dataset.defaultJson || '';
                    if (def) textarea.value = def;
                }
            }
        }
    });

    function createDownloadButton() {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'download-plan-btn';
        btn.textContent = 'Download repair plan';
        btn.addEventListener('click', () => handleDownload(diff));
        return btn;
    }

    function createCopyButton() {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'copy-plan-btn';
        btn.textContent = 'Copy repair plan';
        btn.addEventListener('click', () => handleCopy(diff));
        return btn;
    }

    function handleDownload(diffData) {
        try {
            const selectionInfo = collectSelectionInfo();
            const yaml = buildPlanYaml(diffData, selectionInfo);
            const blob = new Blob([yaml], { type: 'text/yaml' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            const tableKey = diffData.summary.schema + '.' + diffData.summary.table;
            a.download = tableKey.replace('.', '_') + '_repair_plan.yaml';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        } catch (e) {
            alert('Failed to build repair plan: ' + e);
        }
    }

    function handleCopy(diffData) {
        try {
            const selectionInfo = collectSelectionInfo();
            const yaml = buildPlanYaml(diffData, selectionInfo);
            const doCopy = async () => {
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    await navigator.clipboard.writeText(yaml);
                    alert('Repair plan copied to clipboard');
                    return;
                }
                const textarea = document.createElement('textarea');
                textarea.value = yaml;
                textarea.style.position = 'fixed';
                textarea.style.opacity = '0';
                document.body.appendChild(textarea);
                textarea.select();
                document.execCommand('copy');
                document.body.removeChild(textarea);
                alert('Repair plan copied to clipboard');
            };
            doCopy().catch(err => {
                console.error('Copy failed', err);
                alert('Failed to copy repair plan: ' + err);
            });
        } catch (e) {
            alert('Failed to build repair plan: ' + e);
        }
    }

    function collectSelectionInfo() {
        const allCheckboxes = Array.from(document.querySelectorAll('.row-select'));
        const selected = allCheckboxes.filter(cb => cb.checked);
        return {
            selectedKeys: new Set(selected.map(cb => cb.dataset.pk)),
            selectedCount: selected.length,
            totalRows: allCheckboxes.length,
            usingSelection: selected.length > 0
        };
    }

    function buildPlanYaml(diff, selectionInfo) {
        const pkCols = diff.summary.primary_key || [];
        if (!pkCols.length) throw new Error('No primary key info available');
        const tableKey = diff.summary.schema + '.' + diff.summary.table;

        const report = diff.html_report || {};
        const defaultAction = { type: 'keep_n1' };
        const targetKeys = selectionInfo?.usingSelection ? selectionInfo.selectedKeys : null;
        const usingSelection = !!selectionInfo?.usingSelection;
        const allSelected = usingSelection && selectionInfo.selectedCount === selectionInfo.totalRows && selectionInfo.totalRows > 0;

        const rows = collectRows(diff, targetKeys, defaultAction);

        const grouped = groupRows(rows, pkCols);
        const rules = [];
        const overrides = [];

        grouped.forEach(group => {
            const useRulesForGroup = allSelected || group.keys.length > 1;
            const pkMatchers = buildPKMatchers(group.pkTuples, pkCols, !!report.integer_pk);

            if (useRulesForGroup) {
                if (!pkMatchers.length) return;
                rules.push({
                    name: `auto_${group.action.type}_${rules.length + 1}`,
                    pk_in: pkMatchers,
                    diff_type: group.diffType ? [group.diffType] : undefined,
                    action: group.action
                });
                return;
            }

            // Fallback to per-row override for singletons when not all rows are selected.
            const name = 'pk_' + group.keys[0].replace(/\|/g, '_');
            overrides.push({ name, pk: group.pkTuples[0], action: group.action });
        });

        const lines = [];
        lines.push('version: 1');
        lines.push('tables:');
        lines.push('  ' + tableKey + ':');
        lines.push('    default_action:');
        lines.push('      type: ' + defaultAction.type);
        if (rules.length) {
            lines.push('    rules:');
            rules.forEach(rule => {
                lines.push('      - name: ' + quote(rule.name));
                if (rule.pk_in && rule.pk_in.length) {
                    lines.push('        pk_in:');
                    for (const matcher of rule.pk_in) {
                        if (matcher.range) {
                            lines.push('          - range: { from: ' + matcher.range.from + ', to: ' + matcher.range.to + ' }');
                        } else if (matcher.equals && matcher.equals.length) {
                            lines.push('          - equals: ' + formatLiteralList(matcher.equals));
                        }
                    }
                }
                if (rule.diff_type && rule.diff_type.length) {
                    lines.push('        diff_type: ' + formatList(rule.diff_type));
                }
                lines.push('        action:');
                emitActionYaml(lines, '          ', rule.action);
            });
        }
        if (overrides.length) {
            lines.push('    row_overrides:');
            for (const ov of overrides) {
                lines.push('      - name: ' + quote(ov.name));
                lines.push('        pk:');
                pkCols.forEach((col, i) => {
                    lines.push('          ' + col + ': ' + ov.pk[i]);
                });
                lines.push('        action:');
                emitActionYaml(lines, '          ', ov.action);
            }
        }
        return lines.join('\n') + '\n';
    }

    // collectRows turns the shown rows (diff.rows, written by the Go code)
    // into plan entries. A row's pk is a list of JSON literals copied from the
    // diff file; they go into the YAML as they are and are never turned into
    // JavaScript numbers, which would lose digits of a bigint and turn a text
    // key such as "007" into the number 7.
    function collectRows(diff, targetKeys, defaultAction) {
        const rows = [];
        const seen = new Set();

        for (const r of (diff.rows || [])) {
            const key = r.key;
            if (seen.has(key)) continue;
            seen.add(key);
            if (targetKeys && !targetKeys.has(key)) continue;

            let action = selectionForKey(key);
            if (!action) {
                if (r.type === 'missing_on_n2') {
                    action = { type: 'apply_from', from: r.node_a, mode: 'insert' };
                } else if (r.type === 'missing_on_n1') {
                    action = { type: 'apply_from', from: r.node_b, mode: 'insert' };
                } else {
                    action = defaultAction;
                }
            }

            // Skip emitting explicit instructions for rows that match the table default.
            const matchesDefault = isSameAction(action, defaultAction) && r.type === 'row_mismatch';
            if (matchesDefault) continue;

            rows.push({ key, pkTuple: r.pk, action, diffType: r.type });
        }

        return rows;
    }

    function groupRows(rows, pkCols) {
        const groupsByKey = new Map();
        rows.forEach(row => {
            const sig = actionSignature(row.action) + '|' + (row.diffType || '') + '|' + pkCols.length;
            let group = groupsByKey.get(sig);
            if (!group) {
                group = { action: row.action, diffType: row.diffType, pkTuples: [], keys: [] };
                groupsByKey.set(sig, group);
            }
            group.pkTuples.push(row.pkTuple);
            group.keys.push(row.key);
        });
        return Array.from(groupsByKey.values());
    }

    // buildPKMatchers returns pk_in matchers whose values are JSON literals.
    // Ranges are used only when the Go code says that every key of the diff
    // is a whole number (integerPK): then a range over consecutive shown keys
    // cannot match any key that is not shown. BigInt keeps bigint keys exact.
    function buildPKMatchers(pkTuples, pkCols, integerPK) {
        if (!pkTuples.length) return [];
        if (pkCols.length !== 1) {
            // Composite PKs: use equals tuples.
            return [{ equals: pkTuples }];
        }

        const values = Array.from(new Set(pkTuples.map(t => t[0])));
        if (!integerPK) {
            return [{ equals: values }];
        }

        const nums = values.map(v => BigInt(v)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
        const matchers = [];
        const singles = [];
        let start = nums[0];
        let prev = nums[0];
        for (let i = 1; i <= nums.length; i++) {
            const curr = nums[i];
            if (curr !== undefined && curr === prev + 1n) {
                prev = curr;
                continue;
            }
            if (prev > start) {
                matchers.push({ range: { from: start.toString(), to: prev.toString() } });
            } else {
                singles.push(start.toString());
            }
            if (curr !== undefined) {
                start = curr;
                prev = curr;
            }
        }
        if (singles.length) matchers.push({ equals: singles });
        return matchers;
    }

    // formatLiteralList writes a flow list of values that are already YAML
    // literals (JSON text), or lists of them.
    function formatLiteralList(values) {
        return '[' + values.map(v => Array.isArray(v) ? formatLiteralList(v) : v).join(', ') + ']';
    }

    function isSameAction(a, b) {
        if (!a || !b) return false;
        return a.type === b.type && a.from === b.from && a.mode === b.mode;
    }

    function actionSignature(action) {
        const parts = [action.type || ''];
        if (action.from) parts.push('from:' + action.from);
        if (action.mode) parts.push('mode:' + action.mode);
        if (action.custom_row) parts.push('custom:' + JSON.stringify(action.custom_row));
        if (action.helpers) parts.push('helpers:' + JSON.stringify(action.helpers));
        return parts.join('|');
    }

    function formatList(values) {
        if (!Array.isArray(values)) return '[]';
        const rendered = values.map(v => Array.isArray(v) ? formatList(v) : scalar(v));
        return '[' + rendered.join(', ') + ']';
    }

    function emitActionYaml(lines, indent, action) {
        const base = indent + '  ';
        lines.push(indent + 'type: ' + action.type);
        if (action.from) lines.push(indent + 'from: ' + action.from);
        if (action.mode) lines.push(indent + 'mode: ' + action.mode);
        if (action.custom_row !== undefined) {
            lines.push(indent + 'custom_row: ' + toInlineYaml(action.custom_row));
        }
        if (action.helpers) {
            lines.push(indent + 'helpers:');
            if (action.helpers.coalesce_priority && action.helpers.coalesce_priority.length) {
                lines.push(base + 'coalesce_priority: ' + formatList(action.helpers.coalesce_priority));
            }
            if (action.helpers.pick_freshest && action.helpers.pick_freshest.key) {
                lines.push(base + 'pick_freshest:');
                lines.push(base + '  key: ' + scalar(action.helpers.pick_freshest.key));
                if (action.helpers.pick_freshest.tie) {
                    lines.push(base + '  tie: ' + scalar(action.helpers.pick_freshest.tie));
                }
            }
        }
    }

    function toInlineYaml(val) {
        if (val === null || val === undefined) return 'null';
        if (Array.isArray(val) || typeof val === 'object') {
            return JSON.stringify(val);
        }
        return scalar(val);
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
            case 'custom':
                return buildCustomAction(key);
            default:
                return null;
        }
    }

    function buildCustomAction(key) {
        const editor = document.querySelector('.custom-editor[data-pk="' + key + '"]');
        if (!editor) return { type: 'custom' };
        const nodeA = editor.dataset.nodea || 'n1';
        const nodeB = editor.dataset.nodeb || 'n2';

        const customRowInput = editor.querySelector('.custom-row-input');
        let customRow = null;
        const raw = (customRowInput?.value || '').trim();
        if (raw) {
            try {
                customRow = JSON.parse(raw);
            } catch (e) {
                throw new Error('Invalid custom row JSON for ' + key + ': ' + e.message);
            }
        }

        const helpers = {};
        const coalesce = editor.querySelector('.helper-coalesce')?.value || '';
        if (coalesce) helpers.coalesce_priority = coalesce.split(',').map(s => s.trim()).filter(Boolean);

        const freshToggle = editor.querySelector('.helper-freshest-toggle');
        const freshTie = editor.querySelector('.helper-freshest-tie')?.value || nodeA;
        const freshEnabled = !!freshToggle?.checked;
        if (freshEnabled) {
            helpers.pick_freshest = { key: 'commit_ts', tie: freshTie || nodeA };
        }

        const hasHelpers = Object.keys(helpers).length > 0;
        if (!customRow && !hasHelpers) {
            return { type: 'custom' };
        }
        const action = { type: 'custom' };
        if (customRow) action.custom_row = customRow;
        if (hasHelpers) action.helpers = helpers;
        return action;
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
