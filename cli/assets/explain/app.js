/* Turso query plan viewer.
 *
 * Asks the server for a plan as JSON, lays the nodes out as a tree, and draws
 * it bottom-up: leaves at the bottom, arrows pointing at the step that
 * consumes their rows, the finished result on top.
 *
 * Everything here reads the structured fields of the plan (op, table, index,
 * constraints). The `detail` string is only ever shown, never parsed — that is
 * the whole point of the JSON export.
 */

const SVG_NS = 'http://www.w3.org/2000/svg';

const NODE_W = 268;
const NODE_H = 74;
const H_GAP = 26;
const V_GAP = 58;
const PAD = 48;

const EXAMPLES = [
  ['Index range scan', 'SELECT * FROM users u WHERE u.age > 30 ORDER BY u.age'],
  [
    'Join with a sort',
    'SELECT u.name, o.total\n  FROM users u JOIN orders o ON o.user_id = u.id\n WHERE u.age > 30\n ORDER BY o.total DESC',
  ],
  ['Group by', 'SELECT city, count(*) FROM users GROUP BY city ORDER BY 2 DESC'],
  [
    'Correlated subquery',
    'SELECT name, (SELECT count(*) FROM orders o WHERE o.user_id = users.id)\n  FROM users',
  ],
  ['Compound query', 'SELECT name FROM users UNION SELECT city FROM users'],
  [
    'Recursive CTE',
    'WITH RECURSIVE cnt(x) AS (\n  SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 10\n)\nSELECT * FROM cnt',
  ],
  ['OR across two indexes', "SELECT * FROM users WHERE age = 30 OR city = 'Turku'"],
];

/* ---------------------------------------------------------------- presenting
 *
 * One place that turns a plan node into what the diagram shows. Three colors,
 * because more than three cannot be told apart reliably by colorblind readers;
 * the finer distinctions ride on the title text, which every node has.
 */

const SCAN = 'scan';
const SEEK = 'seek';
const FLOW = 'flow';

/** The synthetic node every top-level step feeds. Not part of the plan JSON. */
const RESULT_ID = -1;
const RESULT_OP = '__result__';

function present(node) {
  const table = node.table;
  const target = table ? tableLabel(table) : '';

  switch (node.op) {
    case RESULT_OP:
      return { kind: FLOW, title: 'Query result', target: 'rows returned to the caller', badges: [] };
    case 'Scan': {
      const badges = [];
      if (node.index) {
        badges.push(node.index.covering ? `covering index ${node.index.name}` : `index ${node.index.name}`);
      } else if (table && table.kind === 'table') {
        badges.push('full table scan');
      }
      if (node.left_join) badges.push('left join');
      if (table && table.kind !== 'table') badges.push(kindLabel(table.kind));
      return { kind: SCAN, title: 'Scan', target, badges };
    }
    case 'Search': {
      const badges = [];
      badges.push(node.index ? `index ${node.index.name}` : 'rowid');
      if (node.index && node.index.covering) badges.push('covering');
      if (node.constraints && node.constraints.length) badges.push(node.constraints.join(' AND '));
      if (node.left_join) badges.push('left join');
      return { kind: SEEK, title: 'Search', target, badges };
    }
    case 'MultiIndexScan':
      return {
        kind: SEEK,
        title: node.set_op === 'AND' ? 'Multi-index intersect' : 'Multi-index union',
        target,
        badges: node.indexes || [],
      };
    case 'HashJoin':
      return { kind: SEEK, title: 'Hash join probe', target, badges: [] };
    case 'IndexMethodQuery':
      return { kind: SEEK, title: 'Index method', target, badges: [node.method] };
    case 'ConstantRow':
      return { kind: FLOW, title: 'Constant row', target: 'no FROM clause', badges: [] };
    case 'CompoundQuery':
      return { kind: FLOW, title: 'Compound query', target: '', badges: [] };
    case 'CompoundLeftMost':
      return { kind: FLOW, title: 'First branch', target: '', badges: [] };
    case 'CompoundOperator':
      return {
        kind: FLOW,
        title: node.set_op,
        target: '',
        badges: node.set_op === 'UNION ALL' ? [] : ['temp b-tree'],
      };
    case 'Sort':
      return {
        kind: FLOW,
        title: `Sort for ${node.purpose}`,
        target: '',
        badges: [node.strategy === 'SORTER' ? 'sorter' : 'temp b-tree', 'buffers rows'],
      };
    case 'Distinct':
      return {
        kind: FLOW,
        title: node.aggregate ? `Distinct in ${node.aggregate}()` : 'Distinct',
        target: '',
        badges: ['hash table', 'buffers rows'],
      };
    case 'MaterializeHashBuildInput':
      return {
        kind: FLOW,
        title: 'Materialize hash input',
        target: node.build_table,
        badges: ['buffers rows'],
      };
    case 'Subquery':
      return {
        kind: FLOW,
        title: `${titleCase(node.subquery_kind)} subquery ${node.subquery_id}`,
        target: '',
        badges: node.correlated ? ['correlated'] : ['runs once'],
      };
    case 'RecursiveCte':
      return {
        kind: FLOW,
        title: node.phase === 'SETUP' ? 'Recursive CTE setup' : 'Recursive step',
        target: '',
        badges: [],
      };
    default:
      // A plan node this page has not been taught about still draws, using the
      // text the engine rendered for it.
      return { kind: FLOW, title: node.op, target: node.detail, badges: [] };
  }
}

function tableLabel(table) {
  return table.name === table.identifier ? table.name : `${table.name} AS ${table.identifier}`;
}

function kindLabel(kind) {
  return { virtual_table: 'virtual table', subquery: 'subquery', recursive_cte_input: 'cte input' }[kind] || kind;
}

function titleCase(s) {
  return s.charAt(0) + s.slice(1).toLowerCase();
}

function formatRows(rows) {
  if (rows === undefined || rows === null) return null;
  if (rows < 10) return rows.toFixed(rows < 1 ? 2 : 1).replace(/\.0+$/, '');
  if (rows < 1000) return String(Math.round(rows));
  if (rows < 1e6) return `${(rows / 1e3).toFixed(1).replace(/\.0$/, '')}k`;
  if (rows < 1e9) return `${(rows / 1e6).toFixed(1).replace(/\.0$/, '')}M`;
  return `${(rows / 1e9).toFixed(1).replace(/\.0$/, '')}B`;
}

/* ------------------------------------------------------------------ layout */

/**
 * Turns the flat node list into a tree.
 *
 * A plan has several top-level steps — the tables of a join, then the sorter
 * over them — so the tree gets a synthetic result node on top for all of them
 * to feed. That is what makes the picture one connected flow instead of a row
 * of loose boxes.
 */
function buildTree(nodes) {
  const byId = new Map();
  for (const node of nodes) {
    byId.set(node.id, { node, children: [] });
  }
  const root = { node: { id: RESULT_ID, op: RESULT_OP, detail: 'rows returned to the caller' }, children: [] };
  for (const node of nodes) {
    const entry = byId.get(node.id);
    const parent = node.parent_id === null ? root : byId.get(node.parent_id);
    // A parent id the plan never defined would orphan the node; hang it off
    // the root rather than dropping it.
    (parent || root).children.push(entry);
  }
  return root;
}

/**
 * Places every node.
 *
 * Leaves take the next free column; a parent centers over its children. Depth
 * becomes the row, so the result sits on top and the steps that feed it hang
 * below — rows flow upward along the arrows.
 */
function layout(root) {
  const placed = [];
  let nextColumn = 0;
  let maxDepth = 0;

  function walk(entry, depth) {
    maxDepth = Math.max(maxDepth, depth);
    let column;
    if (entry.children.length === 0) {
      column = nextColumn++;
    } else {
      const columns = entry.children.map((child) => walk(child, depth + 1));
      column = (Math.min(...columns) + Math.max(...columns)) / 2;
    }
    placed.push({ entry, column, depth });
    entry.column = column;
    entry.depth = depth;
    return column;
  }
  walk(root, 0);

  const positions = new Map();
  for (const { entry, column, depth } of placed) {
    positions.set(entry.node.id, {
      x: PAD + column * (NODE_W + H_GAP),
      y: PAD + depth * (NODE_H + V_GAP),
      entry,
    });
  }
  return {
    positions,
    width: PAD * 2 + (nextColumn > 0 ? (nextColumn - 1) * (NODE_W + H_GAP) + NODE_W : NODE_W),
    height: PAD * 2 + maxDepth * (NODE_H + V_GAP) + NODE_H,
    root,
    maxDepth,
  };
}

/* ----------------------------------------------------------------- drawing */

function el(name, attrs = {}, parent = null) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined) node.setAttribute(key, value);
  }
  if (parent) parent.appendChild(node);
  return node;
}

function truncate(text, max) {
  return text.length <= max ? text : `${text.slice(0, max - 1)}…`;
}

class PlanView {
  constructor(svg, wrap) {
    this.svg = svg;
    this.wrap = wrap;
    this.transform = { x: 0, y: 0, k: 1 };
    this.selected = null;
    this.onSelect = () => {};
    this.installPanZoom();
  }

  render(plan) {
    this.plan = plan;
    this.svg.replaceChildren();
    this.selected = null;

    const laidOut = layout(buildTree(plan.nodes));
    this.laidOut = laidOut;

    this.root = el('g', {}, this.svg);
    this.edgeLayer = el('g', {}, this.root);
    this.nodeLayer = el('g', {}, this.root);

    // Biggest row estimate in the plan, so each node's meter is read against
    // the rest of this plan rather than an absolute scale.
    const estimates = plan.nodes.map((n) => n.estimated_rows).filter((r) => typeof r === 'number');
    this.maxRows = estimates.length ? Math.max(...estimates) : 0;

    this.nodesById = new Map(plan.nodes.map((node) => [node.id, node]));
    for (const pos of laidOut.positions.values()) {
      const arrivals = pos.entry.children.length;
      pos.entry.children.forEach((child, i) => {
        const from = laidOut.positions.get(child.node.id);
        if (!from) return;
        // Spread the arrival points across the middle of the parent's bottom
        // edge so a wide fan-in stays readable.
        const spread = Math.min(NODE_W - 60, arrivals * 22);
        const offset = arrivals > 1 ? (i / (arrivals - 1) - 0.5) * spread : 0;
        this.drawEdge(from, pos, offset);
      });
      this.nodesById.set(pos.entry.node.id, pos.entry.node);
      this.drawNode(pos.entry.node, pos);
    }

    this.fit();
  }

  drawEdge(from, to, offset = 0) {
    // Rows leave the child at its top edge and arrive at the parent's bottom.
    const x1 = from.x + NODE_W / 2;
    const y1 = from.y;
    const x2 = to.x + NODE_W / 2 + offset;
    const y2 = to.y + NODE_H + 7;
    const mid = (y1 + y2) / 2;
    el(
      'path',
      { class: 'edge', d: `M ${x1} ${y1} C ${x1} ${mid}, ${x2} ${mid}, ${x2} ${y2}` },
      this.edgeLayer
    );
    el(
      'path',
      { class: 'arrow-head', d: `M ${x2 - 5} ${y2} L ${x2 + 5} ${y2} L ${x2} ${y2 - 7} Z` },
      this.edgeLayer
    );
  }

  drawNode(node, pos) {
    const shown = present(node);
    const group = el(
      'g',
      {
        class: 'node',
        transform: `translate(${pos.x}, ${pos.y})`,
        style: `--accent: var(--${shown.kind})`,
        tabindex: '0',
        role: 'button',
        'aria-label': `${shown.title}. ${node.detail}`,
      },
      this.nodeLayer
    );

    el('rect', { class: 'node-card', width: NODE_W, height: NODE_H, rx: 10 }, group);
    el('rect', { class: 'node-rail', x: 1, y: 9, width: 4, height: NODE_H - 18, rx: 2 }, group);

    const bare = !shown.target && !shown.badges.length;
    el('text', { class: 'node-title', x: 16, y: bare ? NODE_H / 2 + 5 : 22 }, group).textContent =
      truncate(shown.title, 30);

    if (shown.target) {
      el('text', { class: 'node-subtitle', x: 16, y: 40 }, group).textContent = truncate(
        shown.target,
        32
      );
    }

    // Estimated rows: the number, plus a meter so relative size is visible at
    // a glance. Log-scaled, because plans span many orders of magnitude.
    const rows = node.estimated_rows;
    if (typeof rows === 'number') {
      const count = formatRows(rows);
      el('text', { class: 'rows-label', x: NODE_W - 16, y: 22 }, group).textContent =
        `~${count} ${count === '1' ? 'row' : 'rows'}`;
      const trackW = 66;
      const trackX = NODE_W - 16 - trackW;
      el('rect', { class: 'rows-track', x: trackX, y: 28, width: trackW, height: 4, rx: 2 }, group);
      const share = this.maxRows > 1 ? Math.log10(Math.max(rows, 1) + 1) / Math.log10(this.maxRows + 1) : 1;
      el(
        'rect',
        {
          class: 'rows-fill',
          x: trackX,
          y: 28,
          width: Math.max(4, trackW * Math.min(1, share)),
          height: 4,
          rx: 2,
        },
        group
      );
    }

    let badgeX = 16;
    const badgeY = shown.target ? 52 : 36;
    for (const badge of shown.badges) {
      const text = truncate(badge, 26);
      const width = text.length * 6.1 + 12;
      if (badgeX + width > NODE_W - 12) break;
      el(
        'rect',
        { class: 'node-badge-bg', x: badgeX, y: badgeY, width, height: 16, rx: 8 },
        group
      );
      el('text', { class: 'node-badge-text', x: badgeX + 6, y: badgeY + 11.5 }, group).textContent =
        text;
      badgeX += width + 5;
    }

    const select = () => this.select(node.id, group);
    group.addEventListener('click', select);
    group.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        select();
      }
    });
  }

  select(id, group) {
    for (const other of this.nodeLayer.querySelectorAll('.node.selected')) {
      other.classList.remove('selected');
    }
    group.classList.add('selected');
    this.selected = id;
    this.onSelect(this.nodesById.get(id));
  }

  applyTransform() {
    const { x, y, k } = this.transform;
    this.root.setAttribute('transform', `translate(${x}, ${y}) scale(${k})`);
  }

  fit() {
    if (!this.laidOut) return;
    const box = this.wrap.getBoundingClientRect();
    const k = Math.min(1, (box.width - 24) / this.laidOut.width, (box.height - 24) / this.laidOut.height);
    this.transform = {
      k,
      x: (box.width - this.laidOut.width * k) / 2,
      y: (box.height - this.laidOut.height * k) / 2,
    };
    this.applyTransform();
  }

  installPanZoom() {
    let dragging = false;
    let last = null;

    this.wrap.addEventListener('pointerdown', (e) => {
      if (e.target.closest('.node')) return;
      dragging = true;
      last = { x: e.clientX, y: e.clientY };
      this.wrap.classList.add('panning');
      this.wrap.setPointerCapture(e.pointerId);
    });
    this.wrap.addEventListener('pointermove', (e) => {
      if (!dragging) return;
      this.transform.x += e.clientX - last.x;
      this.transform.y += e.clientY - last.y;
      last = { x: e.clientX, y: e.clientY };
      this.applyTransform();
    });
    const stop = (e) => {
      dragging = false;
      this.wrap.classList.remove('panning');
      if (e.pointerId !== undefined && this.wrap.hasPointerCapture?.(e.pointerId)) {
        this.wrap.releasePointerCapture(e.pointerId);
      }
    };
    this.wrap.addEventListener('pointerup', stop);
    this.wrap.addEventListener('pointercancel', stop);

    this.wrap.addEventListener(
      'wheel',
      (e) => {
        if (!this.root) return;
        e.preventDefault();
        const box = this.wrap.getBoundingClientRect();
        const px = e.clientX - box.left;
        const py = e.clientY - box.top;
        const factor = Math.exp(-e.deltaY * 0.0015);
        const k = Math.min(2.5, Math.max(0.15, this.transform.k * factor));
        const scale = k / this.transform.k;
        this.transform.x = px - (px - this.transform.x) * scale;
        this.transform.y = py - (py - this.transform.y) * scale;
        this.transform.k = k;
        this.applyTransform();
      },
      { passive: false }
    );
  }
}

/* -------------------------------------------------------------------- page */

const $ = (id) => document.getElementById(id);

const view = new PlanView($('canvas'), $('canvas-wrap'));
let currentPlan = null;

view.onSelect = (node) => {
  const shown = present(node);
  $('details').hidden = false;
  $('details-title').textContent = shown.title;
  $('details-detail').textContent = node.detail;

  const fields = [];
  if (node.table) {
    fields.push(['Table', tableLabel(node.table)]);
    fields.push(['Kind', kindLabel(node.table.kind)]);
  }
  if (node.index) {
    fields.push(['Index', node.index.name]);
    fields.push(['Covering', node.index.covering ? 'yes — the table is never read' : 'no']);
  }
  if (node.constraints && node.constraints.length) {
    fields.push(['Seek key', node.constraints.join(' AND ')]);
  }
  if (node.indexes) fields.push(['Indexes', node.indexes.join(', ')]);
  if (node.method) fields.push(['Method', node.method]);
  if (node.set_op) fields.push(['Set operation', node.set_op]);
  if (node.purpose) fields.push(['Sorts for', node.purpose]);
  if (node.strategy) fields.push(['Strategy', node.strategy]);
  if (node.aggregate) fields.push(['Aggregate', node.aggregate]);
  if (node.build_table) fields.push(['Build input', node.build_table]);
  if (node.subquery_kind) {
    fields.push(['Subquery', `${node.subquery_kind} #${node.subquery_id}`]);
    fields.push(['Correlated', node.correlated ? 'yes — runs once per outer row' : 'no']);
  }
  if (node.phase) fields.push(['Phase', node.phase]);
  if (node.left_join) fields.push(['Join', 'right side of a LEFT JOIN']);
  if (typeof node.estimated_rows === 'number') {
    fields.push(['Estimated rows', `${formatRows(node.estimated_rows)} per outer row`]);
  }
  if (node.id !== RESULT_ID) fields.push(['Node id', String(node.id)]);

  const dl = $('details-fields');
  dl.replaceChildren();
  for (const [key, value] of fields) {
    const dt = document.createElement('dt');
    dt.textContent = key;
    const dd = document.createElement('dd');
    dd.textContent = value;
    dl.append(dt, dd);
  }
};

$('details-close').addEventListener('click', () => {
  $('details').hidden = true;
});

async function explain() {
  const sql = $('sql').value.trim();
  if (!sql) return;
  $('error').hidden = true;
  try {
    const response = await fetch('/api/plan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }),
    });
    const payload = await response.json();
    if (payload.error) {
      showError(payload.error);
      return;
    }
    currentPlan = payload;
    $('empty').hidden = payload.nodes.length > 0;
    if (!payload.nodes.length) {
      $('empty').textContent = 'This statement has no plan steps.';
      $('canvas').replaceChildren();
      $('table-body').replaceChildren();
      return;
    }
    $('details').hidden = true;
    view.render(payload);
    renderTable(payload);
  } catch (e) {
    showError(String(e));
  }
}

function showError(message) {
  const box = $('error');
  box.textContent = message;
  box.hidden = false;
}

function renderTable(plan) {
  const body = $('table-body');
  body.replaceChildren();
  for (const node of plan.nodes) {
    const shown = present(node);
    const tr = document.createElement('tr');

    const id = document.createElement('td');
    id.textContent = node.id;
    id.className = 'num';

    const parent = document.createElement('td');
    parent.textContent = node.parent_id === null ? '—' : node.parent_id;
    parent.className = 'num';

    const kind = document.createElement('td');
    const pill = document.createElement('span');
    pill.className = 'kind-pill';
    pill.style.setProperty('--pill', `var(--${shown.kind})`);
    const dot = document.createElement('i');
    pill.append(dot, document.createTextNode(shown.title));
    kind.append(pill);

    const step = document.createElement('td');
    step.className = 'step';
    step.textContent = node.detail;

    const rows = document.createElement('td');
    rows.className = 'num';
    rows.textContent = typeof node.estimated_rows === 'number' ? formatRows(node.estimated_rows) : '—';

    tr.append(id, parent, kind, step, rows);
    body.append(tr);
  }
}

$('explain').addEventListener('click', explain);
$('sql').addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
    e.preventDefault();
    explain();
  }
});
$('fit').addEventListener('click', () => view.fit());
window.addEventListener('resize', () => view.fit());

$('toggle-table').addEventListener('click', (e) => {
  const showing = e.currentTarget.getAttribute('aria-pressed') === 'true';
  e.currentTarget.setAttribute('aria-pressed', String(!showing));
  $('table-view').hidden = showing;
  $('canvas-wrap').hidden = !showing;
  if (showing) view.fit();
});

$('copy-json').addEventListener('click', async () => {
  if (!currentPlan) return;
  await navigator.clipboard.writeText(JSON.stringify(currentPlan, null, 2));
  const button = $('copy-json');
  const label = button.textContent;
  button.textContent = 'Copied';
  setTimeout(() => {
    button.textContent = label;
  }, 1200);
});

for (const [name, sql] of EXAMPLES) {
  const li = document.createElement('li');
  const button = document.createElement('button');
  button.type = 'button';
  button.textContent = name;
  button.addEventListener('click', () => {
    $('sql').value = sql;
    explain();
  });
  li.append(button);
  $('examples').append(li);
}

fetch('/api/schema')
  .then((r) => r.json())
  .then((schema) => {
    $('db-name').textContent = schema.database || '';
    const list = $('schema');
    list.replaceChildren();
    if (!schema.tables || !schema.tables.length) {
      const li = document.createElement('li');
      li.className = 'muted';
      li.textContent = 'no tables yet';
      list.append(li);
      return;
    }
    for (const table of schema.tables) {
      const li = document.createElement('li');
      const name = document.createElement('span');
      name.textContent = table.name;
      const cols = document.createElement('span');
      cols.className = 'cols';
      cols.textContent = ` (${table.columns.join(', ')})`;
      li.append(name, cols);
      list.append(li);
    }
  })
  .catch(() => {
    $('db-name').textContent = '';
  });
