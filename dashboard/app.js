const $ = (id) => document.getElementById(id);
let busy = false;
let latestNodes = [];
let pendingConfirm = null;

async function api(url, options = {}) {
  const response = await fetch(url, { ...options, headers: { ...(options.headers || {}), 'Content-Type': 'application/json' } });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || data.message || `Request failed (${response.status})`);
  return data;
}
function setNotice(message, type = 'info') {
  const el = $('notice'); el.textContent = message; el.className = `notice ${type}`;
}
function setBusy(value) {
  busy = value;
  document.querySelectorAll('button').forEach(b => { if (!b.closest('.modal')) b.disabled = value; });
}
function timeLabel(value) {
  if (!value) return 'Time unavailable';
  const d = new Date(value); return Number.isNaN(d.getTime()) ? 'Time unavailable' : d.toLocaleTimeString();
}
function makeButton(label, cls, handler, title = '') {
  const b = document.createElement('button'); b.className = `btn ${cls}`; b.textContent = label; b.title = title || label; b.addEventListener('click', handler); return b;
}
function renderNodes(nodes) {
  const container = $('nodes'); container.replaceChildren();
  if (!nodes.length) {
    const empty = document.createElement('div'); empty.className = 'empty-state'; empty.innerHTML = 'Cluster is stopped. Click <b>Start Cluster</b> to launch Nodes 1, 2 and 3.'; container.appendChild(empty); return;
  }
  for (const node of nodes) {
    const card = document.createElement('article'); card.className = `node-card ${node.is_leader && node.status === 'healthy' ? 'is-leader' : ''}`;
    const top = document.createElement('div'); top.className = 'node-top';
    const title = document.createElement('div'); title.className = 'node-title';
    const icon = document.createElement('div'); icon.className = 'node-icon'; icon.textContent = String(node.id).padStart(2, '0');
    const headingWrap = document.createElement('div'); const h = document.createElement('h3'); h.textContent = `Node ${node.id}`; const port = document.createElement('p'); port.className = 'node-port'; port.textContent = `127.0.0.1:${node.port}`; headingWrap.append(h, port); title.append(icon, headingWrap);
    const status = document.createElement('span'); status.className = `status ${node.status}`; status.textContent = node.status === 'healthy' ? 'HEALTHY' : node.status === 'unhealthy' ? 'FAILED' : 'STARTING'; top.append(title, status); card.append(top);
    const clock = document.createElement('div'); clock.className = 'node-lamport';
    clock.innerHTML = `<span>Lamport clock</span><strong>L = ${Number.isInteger(node.lamport_clock) ? node.lamport_clock : '—'}</strong>`;
    card.append(clock);
    const vector = document.createElement('div'); vector.className = 'node-vector';
    const vectorLabel = document.createElement('span'); vectorLabel.textContent = 'Vector clock';
    const vectorValue = document.createElement('strong'); vectorValue.textContent = Array.isArray(node.vector_clock) ? `[${node.vector_clock.join(', ')}]` : '—';
    vector.append(vectorLabel, vectorValue); card.append(vector);
    const leader = document.createElement('div'); leader.className = 'reported-leader';
    if (node.status === 'healthy') { leader.textContent = node.is_leader ? `★ Node ${node.id} is the coordinator` : `Reported leader: ${node.leader_id ? `Node ${node.leader_id}` : 'Election pending'}`; if (node.is_leader) leader.classList.add('leader-label'); }
    else leader.textContent = node.status === 'unhealthy' ? 'Process is running; failure is simulated' : 'Waiting for node response';
    card.append(leader);
    const actions = document.createElement('div'); actions.className = 'node-actions';
    if (node.status === 'healthy') {
      actions.append(makeButton('Simulate Failure', 'btn-warning', () => runNodeAction(node.id, 'failure'), 'Mark node unavailable without stopping its process'));
    } else if (node.status === 'unhealthy') {
      actions.append(makeButton('Recover Node', 'btn-success', () => runNodeAction(node.id, 'recover')));
    } else {
      actions.append(makeButton('Refresh Status', 'btn-secondary', refreshDashboard));
    }
    actions.append(makeButton('Remove Process', 'btn-danger-outline', () => askRemove(node.id), 'Stop and remove this node process'));
    card.append(actions); container.append(card);
  }
}
function renderTopology(nodes, events = []) {
  const container = $('topology');
  container.replaceChildren();
  if (!nodes.length) {
    const empty = document.createElement('div'); empty.className = 'topology-empty'; empty.textContent = 'Start the cluster to see node health and coordinator links in real time.'; container.append(empty);
    $('topology-caption').textContent = 'No active cluster is being managed.';
    updateElectionFlow(events);
    return;
  }
  const width = 900, height = 270, cx = width / 2, cy = 128;
  const radius = nodes.length <= 4 ? 82 : nodes.length <= 7 ? 96 : 105;
  const nodeRadius = nodes.length > 7 ? 25 : 30;
  const positions = new Map();
  nodes.forEach((node, index) => {
    const angle = (-Math.PI / 2) + (index * 2 * Math.PI / nodes.length);
    positions.set(node.id, { x: cx + radius * Math.cos(angle), y: cy + radius * Math.sin(angle) });
  });
  const leaders = nodes.filter(n => n.status === 'healthy' && n.is_leader);
  const leader = leaders.length === 1 ? leaders[0] : null;
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`); svg.setAttribute('role', 'img');
  svg.setAttribute('aria-label', 'Live PulseNet cluster topology');
  const defs = document.createElementNS(ns, 'defs');
  const marker = document.createElementNS(ns, 'marker'); marker.setAttribute('id','arrowhead'); marker.setAttribute('markerWidth','7'); marker.setAttribute('markerHeight','7'); marker.setAttribute('refX','5'); marker.setAttribute('refY','3'); marker.setAttribute('orient','auto'); marker.setAttribute('markerUnits','strokeWidth');
  const arrow = document.createElementNS(ns, 'path'); arrow.setAttribute('d','M0,0 L6,3 L0,6 z'); arrow.setAttribute('fill','#6ba8ff'); marker.append(arrow); defs.append(marker); svg.append(defs);
  const edgeLayer = document.createElementNS(ns, 'g');
  if (leader) {
    const start = positions.get(leader.id);
    nodes.filter(n => n.id !== leader.id).forEach(node => {
      const end = positions.get(node.id);
      const dx = end.x - start.x, dy = end.y - start.y, length = Math.hypot(dx,dy) || 1;
      const sx = start.x + dx / length * (nodeRadius + 5), sy = start.y + dy / length * (nodeRadius + 5);
      const ex = end.x - dx / length * (nodeRadius + 8), ey = end.y - dy / length * (nodeRadius + 8);
      const path = document.createElementNS(ns,'path'); path.setAttribute('d',`M ${sx} ${sy} L ${ex} ${ey}`);
      path.setAttribute('class', `topology-edge ${node.status === 'healthy' ? 'leader-edge' : 'failed-edge'}`);
      if (node.status === 'healthy') path.setAttribute('marker-end','url(#arrowhead)'); edgeLayer.append(path);
    });
  } else {
    // No single coordinator: show peer links as dashed lines until the election converges.
    nodes.forEach((node, i) => { if (i < nodes.length - 1) { const a=positions.get(node.id), b=positions.get(nodes[i+1].id); const line=document.createElementNS(ns,'line'); line.setAttribute('x1',a.x); line.setAttribute('y1',a.y); line.setAttribute('x2',b.x); line.setAttribute('y2',b.y); line.setAttribute('class','topology-edge failed-edge'); edgeLayer.append(line); } });
  }
  svg.append(edgeLayer);
  const centerLabel = document.createElementNS(ns,'text'); centerLabel.setAttribute('x',cx); centerLabel.setAttribute('y',cy-6); centerLabel.setAttribute('class','topology-center-label'); centerLabel.textContent = leader ? `NODE ${leader.id}` : (leaders.length > 1 ? 'LEADER CONFLICT' : 'ELECTION'); svg.append(centerLabel);
  const centerSub = document.createElementNS(ns,'text'); centerSub.setAttribute('x',cx); centerSub.setAttribute('y',cy+12); centerSub.setAttribute('class','topology-center-label'); centerSub.textContent = leader ? 'COORDINATOR' : 'IN PROGRESS'; svg.append(centerSub);
  nodes.forEach(node => {
    const pos = positions.get(node.id); const group = document.createElementNS(ns,'g');
    const classes = ['topology-node']; if (node.status === 'healthy') classes.push('healthy'); else if (node.status === 'unhealthy') classes.push('failed'); else classes.push('starting'); if (node.is_leader && node.status === 'healthy') classes.push('leader');
    group.setAttribute('class',classes.join(' ')); group.setAttribute('transform',`translate(${pos.x},${pos.y})`);
    const circle = document.createElementNS(ns,'circle'); circle.setAttribute('r',nodeRadius); group.append(circle);
    const label = document.createElementNS(ns,'text'); label.setAttribute('y','-2'); label.textContent = `N${node.id}`; group.append(label);
    const state = document.createElementNS(ns,'text'); state.setAttribute('class','node-state'); state.setAttribute('y','13'); state.textContent = node.status === 'healthy' ? (node.is_leader ? 'LEADER' : 'ONLINE') : node.status === 'unhealthy' ? 'FAILED' : 'STARTING'; group.append(state);
    const title = document.createElementNS(ns,'title'); title.textContent = `Node ${node.id} — ${node.status}${node.is_leader ? ', current coordinator' : ''}`; group.append(title); svg.append(group);
  });
  container.append(svg);
  $('topology-caption').textContent = leader ? `Node ${leader.id} is coordinating ${nodes.filter(n => n.status === 'healthy').length} healthy node(s). The map refreshes automatically.` : 'No single healthy coordinator is confirmed yet. The dashed links indicate that an election may be in progress.';
  updateElectionFlow(events);
}
function updateElectionFlow(events) {
  const recent = events.slice(-40).map(e => String(e.message || '').toLowerCase());
  const latestElection = Math.max(recent.findLastIndex(m => m.includes('election started') || m.includes('election triggered')), recent.findLastIndex(m => m.includes('election request received')));
  const relevant = latestElection >= 0 ? recent.slice(latestElection) : recent;
  const flags = {
    request: relevant.some(m => m.includes('election started') || m.includes('election request received') || m.includes('election triggered')),
    response: relevant.some(m => m.includes('higher nodes responded') || m.includes('acknowledged election request')),
    leader: relevant.some(m => m.includes('elected as leader')),
    announce: relevant.some(m => m.includes('coordinator changed') || m.includes('coordinator updated'))
  };
  const order = ['request','response','leader','announce'];
  let latest = -1; order.forEach((key,i) => { if (flags[key]) latest = i; });
  document.querySelectorAll('.flow-step').forEach((el,i) => { const key=el.dataset.flow; el.classList.toggle('complete', flags[key] && i < latest); el.classList.toggle('active', flags[key] && i === latest); });
}
function renderLamportClocks(nodes) {
  const container = $('lamport-clocks');
  container.replaceChildren();
  if (!nodes.length) {
    const empty = document.createElement('div'); empty.className = 'empty-state'; empty.textContent = 'Start the cluster to observe logical clocks.'; container.append(empty); return;
  }
  for (const node of nodes) {
    const card = document.createElement('article');
    card.className = `lamport-card ${node.is_leader && node.status === 'healthy' ? 'is-leader' : ''}`;
    const top = document.createElement('div'); top.className = 'lamport-card-top';
    const name = document.createElement('strong'); name.textContent = `Node ${node.id}`;
    const status = document.createElement('span'); status.className = `status ${node.status}`; status.textContent = node.status === 'healthy' ? 'ACTIVE' : node.status === 'unhealthy' ? 'FAILED' : 'OFFLINE';
    top.append(name, status);
    const value = document.createElement('div'); value.className = 'lamport-value';
    value.textContent = Number.isInteger(node.lamport_clock) ? `L = ${node.lamport_clock}` : 'L = —';
    const detail = document.createElement('p'); detail.className = 'lamport-detail';
    detail.textContent = node.is_leader && node.status === 'healthy' ? 'Current coordinator' : (node.status === 'healthy' ? 'Logical counter' : 'Clock unavailable while node is offline');
    card.append(top, value, detail); container.append(card);
  }
}
function renderVectorClocks(nodes) {
  const container = $('vector-clocks');
  container.replaceChildren();
  if (!nodes.length) {
    const empty = document.createElement('div'); empty.className = 'empty-state'; empty.textContent = 'Start the cluster to observe vector clocks.'; container.append(empty); return;
  }
  for (const node of nodes) {
    const card = document.createElement('article');
    card.className = `vector-card ${node.is_leader && node.status === 'healthy' ? 'is-leader' : ''}`;
    const top = document.createElement('div'); top.className = 'lamport-card-top';
    const name = document.createElement('strong'); name.textContent = `Node ${node.id}`;
    const status = document.createElement('span'); status.className = `status ${node.status}`; status.textContent = node.status === 'healthy' ? 'ACTIVE' : node.status === 'unhealthy' ? 'FAILED' : 'OFFLINE';
    top.append(name, status);
    const value = document.createElement('div'); value.className = 'vector-value';
    value.textContent = Array.isArray(node.vector_clock) ? `[${node.vector_clock.join(', ')}]` : 'V = —';
    const detail = document.createElement('p'); detail.className = 'lamport-detail';
    detail.textContent = `Component order: N1, N2, N3${node.vector_clock && node.vector_clock.length > 3 ? ', …' : ''}`;
    card.append(top, value, detail); container.append(card);
  }
}
async function renderEvents() {
  const container = $('events');
  try {
    const data = await api('/api/events'); const events = (data.events || []).slice().reverse(); renderTopology(latestNodes, data.events || []); container.replaceChildren();
    if (!events.length) { const empty = document.createElement('div'); empty.className = 'empty-state'; empty.textContent = 'No events yet. Start the cluster or perform an operation.'; container.append(empty); return; }
    for (const event of events) {
      const row = document.createElement('div'); row.className = 'event-row';
      const time = document.createElement('span'); time.className = 'event-time'; time.textContent = timeLabel(event.time);
      const tag = document.createElement('span'); tag.className = `event-tag ${event.node_id ? '' : 'system'}`; tag.textContent = event.node_id ? `NODE ${event.node_id}` : 'SYSTEM';
      const logical = document.createElement('span'); logical.className = 'event-lamport'; logical.textContent = Number.isInteger(event.lamport_clock) ? `L=${event.lamport_clock}` : '—'; logical.title = 'Lamport logical timestamp (not wall-clock time)';
      const vector = document.createElement('span'); vector.className = 'event-vector'; vector.textContent = Array.isArray(event.vector_clock) ? `V=[${event.vector_clock.join(',')}]` : ''; vector.title = 'Vector timestamp: per-node causal knowledge';
      const message = document.createElement('span'); message.className = 'event-message'; message.textContent = event.message;
      row.append(time, tag, logical, vector, message); container.append(row);
    }
  } catch (error) { container.textContent = `Could not load events: ${error.message}`; }
}
async function refreshDashboard() {
  try {
    const data = await api('/api/cluster'); latestNodes = data.nodes || []; renderNodes(latestNodes); renderLamportClocks(latestNodes); renderVectorClocks(latestNodes);
    $('total-nodes').textContent = data.total; $('healthy-nodes').textContent = data.healthy; $('leader').textContent = data.leader;
    $('cluster-status').textContent = !data.running ? 'Cluster stopped' : data.healthy === data.total ? 'All Nodes Online' : `${data.healthy}/${data.total} Nodes Healthy`;
    $('node-count').textContent = `${data.total} node${data.total === 1 ? '' : 's'}`;
    $('add-node').disabled = busy || !data.running || data.total >= data.max_nodes;
    $('trigger-election').disabled = busy || data.healthy === 0;
    $('stop-cluster').disabled = busy || !data.running;
    $('start-cluster').disabled = busy || data.running;
  } catch (error) { $('cluster-status').textContent = 'Dashboard API unavailable'; setNotice(error.message, 'error'); }
  await renderEvents();
}
async function performAction(label, fn) {
  if (busy) return; setBusy(true); setNotice(`${label}…`, 'info');
  try { const result = await fn(); setNotice(result.message || `${label} complete.`, 'success'); await refreshDashboard(); }
  catch (error) { setNotice(error.message, 'error'); }
  finally { setBusy(false); await refreshDashboard(); }
}
async function runNodeAction(nodeId, action) {
  const label = action === 'failure' ? `Simulating failure on Node ${nodeId}` : `Recovering Node ${nodeId}`;
  await performAction(label, () => api(`/api/nodes/${nodeId}/${action}`, { method: 'POST', body: '{}' }));
}
function askRemove(nodeId) {
  pendingConfirm = nodeId; $('confirm-title').textContent = `Remove Node ${nodeId}?`; $('confirm-message').textContent = `This stops the Node ${nodeId} process and frees port ${8000 + nodeId}. Remaining healthy nodes will elect a leader if needed.`; $('confirm-modal').classList.remove('hidden');
}
function closeConfirm() { $('confirm-modal').classList.add('hidden'); pendingConfirm = null; }
$('cancel-confirm').addEventListener('click', closeConfirm);
$('accept-confirm').addEventListener('click', async () => { const id = pendingConfirm; closeConfirm(); if (id !== null) await performAction(`Removing Node ${id}`, () => api(`/api/nodes/${id}`, { method: 'DELETE' })); });
$('start-cluster').addEventListener('click', () => performAction('Starting cluster', () => api('/api/cluster/start', { method: 'POST', body: '{}' })));
$('stop-cluster').addEventListener('click', () => performAction('Stopping cluster', () => api('/api/cluster/stop', { method: 'POST', body: '{}' })));
$('add-node').addEventListener('click', () => performAction('Adding node', () => api('/api/nodes/add', { method: 'POST', body: '{}' })));
$('trigger-election').addEventListener('click', () => performAction('Triggering Bully election', () => api('/api/election', { method: 'POST', body: '{}' })));
$('refresh-button').addEventListener('click', refreshDashboard);
refreshDashboard(); setInterval(() => { if (!busy) refreshDashboard(); }, 2000);
