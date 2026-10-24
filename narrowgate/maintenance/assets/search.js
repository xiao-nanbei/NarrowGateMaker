/* Private, offline-only snapshot search. No filesystem writes or network APIs. */
(() => {
  'use strict';
  const manifest = window.NG_FILE_SEARCH;
  const $ = id => document.getElementById(id);
  const form = $('file-search-form');
  const query = $('file-search-query');
  const submit = $('file-search-submit');
  const cancel = $('file-search-cancel');
  const progress = $('file-search-progress');
  const results = $('file-search-results');
  const LIMIT = 200;
  let running = false;
  let stopped = false;
  const format = n => n.toLocaleString('zh-CN');
  const normalized = s => s.normalize('NFKC').toLocaleLowerCase('en-US');
  const pause = () => new Promise(resolve => setTimeout(resolve, 0));

  function bytes(n) {
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    let u = 0, value = n;
    while (value >= 1024 && u < units.length - 1) { value /= 1024; u++; }
    return `${value.toFixed(u ? 2 : 0)} ${units[u]}`;
  }

  function verdict(category) {
    if (category.startsWith('保留：') || category.includes('用户指定') || category.includes('已验收prepare归档') || category.includes('源码或依赖')) {
      return {kind: 'keep', title: '应保留', reason: `清单已有保留依据：${category}。除非用途变化并获得相应授权，否则不要清理。`};
    }
    if (category.includes('检查点候选')) {
      return {kind: 'candidate', title: '清理候选 · 需核验', reason: '先验收完整经济结果、终点会计、进程退出及无恢复引用；进行中的任务必须保留最新有效恢复点。'};
    }
    if (category.includes('日志候选')) {
      return {kind: 'candidate', title: '清理候选 · 需核验', reason: '仅为日志候选。需确认没有进程打开、不属于唯一故障或实盘证据、无当前依赖，再确定是否删除。'};
    }
    if (category.includes('可重建缓存')) {
      return {kind: 'candidate', title: '清理候选 · 需核验', reason: '需确认可从保留来源恢复，排除活动 release、运行／排队／恢复依赖；prepare 还须核验归档原版本和适配器恢复路径。'};
    }
    if (category.includes('Finder缓存')) {
      return {kind: 'candidate', title: '低风险候选 · 需核验', reason: '清单识别为 Finder 展示缓存；确认实际路径与用途一致后通常可清理，不据此删除同目录数据。'};
    }
    if (category.includes('锁用途')) {
      return {kind: 'review', title: '用途待核验', reason: '先确认是运行锁还是依赖锁；活动运行锁和 requirements／uv／poetry 等依赖锁不能按“旧文件”删除。'};
    }
    return {kind: 'review', title: '用途待核验', reason: '现有清单不足以证明可删除。需核对实际内容、来源／替代副本、任务依赖与保留要求；原始数据、模型、正式结果及唯一证据默认保留。'};
  }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  async function copyPath(path, button) {
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(path);
      } else {
        const area = element('textarea', '', path);
        area.style.cssText = 'position:fixed;left:-10000px;top:0';
        document.body.append(area);
        area.select();
        const ok = document.execCommand('copy');
        area.remove();
        if (!ok) throw new Error('copy');
      }
      button.textContent = '已复制路径';
    } catch (_) { button.textContent = '请选中上方路径复制'; }
  }

  function addResult(row, decision) {
    const path = manifest.roots[row[0]] + '/' + row[1];
    const card = element('article', 'result-card');
    const top = element('div', 'result-top');
    top.append(element('span', 'result-name', row[1].split('/').pop()), element('span', `verdict ${decision.kind}`, decision.title));
    const location = manifest.locations[row[0]];
    card.append(top, element('p', 'result-path', path),
      element('p', 'result-stats', `${location} · ${bytes(row[3])}（${format(row[3])} 字节） · 修改时间 ${row[2]} · 逻辑大小，非可释放空间`),
      element('p', 'result-reason', `原分类：${manifest.categories[row[4]]}。${decision.reason}`));
    const actions = element('div', 'result-actions');
    const copy = element('button', '', '复制路径');
    copy.type = 'button';
    copy.addEventListener('click', () => copyPath(path, copy));
    actions.append(copy);
    const group = manifest.groups[row[5]];
    if (group) {
      const link = element('a', '', '浏览所属月份分页');
      link.href = group;
      link.target = '_blank';
      link.rel = 'noopener';
      actions.append(link);
    }
    card.append(actions);
    results.append(card);
  }

  function loadChunk(part) {
    return new Promise((resolve, reject) => {
      const script = document.createElement('script');
      const timer = setTimeout(() => finish(new Error(`索引载入超时：${part.file}`)), 15000);
      function finish(error, value) {
        clearTimeout(timer);
        script.onload = script.onerror = null;
        script.remove();
        delete window.NG_FILE_CHUNK;
        if (error) reject(error); else resolve(value);
      }
      script.src = 'search/' + part.file;
      script.onload = () => {
        const data = window.NG_FILE_CHUNK;
        if (!data || data[0] !== part.file) finish(new Error('索引内容不完整，请重建搜索索引。'));
        else finish(null, data[1]);
      };
      script.onerror = () => finish(new Error(`无法载入索引：${part.file}。请保留整个轻量分页目录，并重新打开首页。`));
      document.head.append(script);
    });
  }

  async function decode(encoded) {
    const binary = atob(encoded);
    const buffer = Uint8Array.from(binary, c => c.charCodeAt(0));
    const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream('gzip'));
    return new Response(stream).json();
  }

  if (!manifest || manifest.schema !== 1) {
    $('file-search-meta').textContent = '搜索索引未载入。请保留 search 文件夹与首页的相对位置，原有分页仍可浏览。';
    submit.disabled = true;
    return;
  }
  $('file-search-meta').textContent = `覆盖 ${format(manifest.count)} 条文件记录（不含 .git）；基础扫描 ${manifest.scannedAt}；清理同步 ${manifest.updatedAt}。后续清单变更需重新生成搜索索引。`;
  if (typeof DecompressionStream === 'undefined') {
    progress.textContent = '当前浏览器不支持离线压缩索引，请使用新版 Chrome、Edge 或 Safari；下方分页仍可浏览。';
    submit.disabled = true;
    return;
  }
  for (const label of new Set(manifest.locations)) {
    const option = element('option', '', label);
    option.value = label;
    $('file-search-root').append(option);
  }
  const decisions = manifest.categories.map(verdict);
  cancel.addEventListener('click', () => { stopped = true; cancel.disabled = true; });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    if (running) return;
    const tokens = normalized(query.value.trim()).split(/\s+/u).filter(Boolean);
    if (!tokens.length) { query.focus(); return; }
    const field = $('file-search-field').value;
    const rootFilter = $('file-search-root').value;
    const statusFilter = $('file-search-status').value;
    running = true; stopped = false;
    submit.disabled = true; cancel.hidden = false; cancel.disabled = false;
    results.replaceChildren(); results.setAttribute('aria-busy', 'true');
    progress.textContent = `正在检索 0 / ${format(manifest.count)} 条，可随时停止。`;
    let scanned = 0, matches = 0, totalBytes = 0, shown = 0;
    const tally = {candidate: 0, review: 0, keep: 0};
    const eligibleRoots = manifest.locations.map(label => rootFilter === 'all' || label === rootFilter);
    let failed = null;
    try {
      for (const part of manifest.parts) {
        if (stopped) break;
        const rows = await decode(await loadChunk(part));
        if (rows.length !== part.rows) throw new Error('索引记录数不符，请重新生成索引。');
        for (const row of rows) {
          if (stopped) break;
          scanned++;
          const decision = decisions[row[4]];
          if (!eligibleRoots[row[0]] || (statusFilter !== 'all' && decision.kind !== statusFilter)) continue;
          const value = normalized(field === 'name' ? row[1].split('/').pop() : manifest.roots[row[0]] + '/' + row[1]);
          if (!tokens.every(token => value.includes(token))) continue;
          matches++; totalBytes += row[3]; tally[decision.kind]++;
          if (shown < LIMIT) { addResult(row, decision); shown++; }
        }
        progress.textContent = `正在检索 ${format(scanned)} / ${format(manifest.count)} 条（${Math.round(scanned / manifest.count * 100)}%），已找到 ${format(matches)} 条。可随时停止。`;
        await pause();
      }
    } catch (error) { failed = error.message; }
    finally {
      running = false; submit.disabled = false; cancel.hidden = true; results.setAttribute('aria-busy', 'false');
      const prefix = failed ? `检索未完成：${failed}` : stopped ? '已停止，以下仅为部分结果。' : '检索完成。';
      const detail = matches ? `匹配 ${format(matches)} 条，逻辑大小合计 ${bytes(totalBytes)}（不代表可释放空间）。\n清理候选 ${format(tally.candidate)} · 用途待核验 ${format(tally.review)} · 应保留 ${format(tally.keep)}。` : '当前已检索范围未找到匹配记录；不代表磁盘不存在或可以删除。';
      progress.textContent = prefix + ' ' + detail + (matches > LIMIT ? `\n仅展示前 ${LIMIT} 条；请增加关键词或使用筛选缩小范围。` : '');
    }
  });
})();
