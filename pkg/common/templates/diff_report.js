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
