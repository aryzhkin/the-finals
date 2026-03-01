const puppeteer = require('puppeteer');

const BASE = 'http://localhost:8079';
const PAGES = [
  'overview', 'community-insights', 'season-health', 'player-journey',
  'praise-vs-complaints', 'entity-tracker', 'category-deep-dive',
  'regional-analysis', 'top-reviews', 'review-explorer', 'word-cloud',
  'review-bombing', 'patch-notes', 'methodology', 'about'
];

const wait = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();

  const errors = [];
  page.on('console', msg => {
    if (msg.type() === 'error') errors.push(msg.text());
  });
  page.on('pageerror', err => {
    errors.push(err.message);
  });

  let totalTests = 0, passedTests = 0;
  const failures = [];

  function test(name, condition, detail) {
    totalTests++;
    if (condition) {
      passedTests++;
    } else {
      const msg = `FAIL: ${name}${detail ? ' — ' + detail : ''}`;
      failures.push(msg);
      console.log('  ' + msg);
    }
  }

  // === Test 1: Load and render all 15 pages ===
  console.log('=== Test 1: Page loading ===');
  await page.goto(BASE, { waitUntil: 'networkidle0', timeout: 30000 });
  await wait(1000);

  const dataLoaded = await page.evaluate(() => typeof DATA !== 'undefined' && typeof DATA.overview === 'object');
  test('DATA loaded', dataLoaded);

  for (const pageId of PAGES) {
    await page.evaluate((id) => { window.location.hash = '#' + id; }, pageId);
    await wait(400);

    const isActive = await page.evaluate((id) => {
      const el = document.getElementById('page-' + id);
      return el && el.classList.contains('active');
    }, pageId);
    test(`Page ${pageId} activates`, isActive);

    const content = await page.evaluate((id) => {
      const el = document.getElementById('page-' + id);
      return el ? el.innerHTML.length : 0;
    }, pageId);
    test(`Page ${pageId} has content`, content > 100, `${content} chars`);
  }

  // === Test 2: Patch Notes specifics ===
  console.log('\n=== Test 2: Patch Notes ===');
  await page.evaluate(() => { window.location.hash = '#patch-notes'; });
  await wait(500);

  const pillCount = await page.evaluate(() =>
    document.querySelectorAll('#pn-season-pills .pill').length
  );
  test('Season pills = 9', pillCount === 9, `got ${pillCount}`);

  const activePill = await page.evaluate(() => {
    const a = document.querySelector('#pn-season-pills .pill.active');
    return a ? a.textContent : null;
  });
  test('Default active pill is S9', activePill === 'S9', `got ${activePill}`);

  const filterValue = await page.evaluate(() => {
    const sel = document.getElementById('pn-impact-filter');
    return sel ? sel.value : null;
  });
  test('Impact filter defaults to S9', filterValue === 'S9', `got ${filterValue}`);

  const visibleImpactRows = await page.evaluate(() => {
    const rows = document.querySelectorAll('#pn-impact-table tbody tr');
    let v = 0; rows.forEach(r => { if (r.style.display !== 'none') v++; }); return v;
  });
  test('Impact Summary has visible rows', visibleImpactRows > 0, `${visibleImpactRows} visible`);

  const optgroupCount = await page.evaluate(() =>
    document.querySelectorAll('#pn-entity-select optgroup').length
  );
  test('Entity selector has optgroups', optgroupCount >= 5, `${optgroupCount} groups`);

  const firstGroupItems = await page.evaluate(() => {
    const g = document.querySelector('#pn-entity-select optgroup');
    return g ? [...g.querySelectorAll('option')].map(o => o.value) : [];
  });
  const isSorted = firstGroupItems.every((v, i, a) => i === 0 || a[i-1].localeCompare(v) <= 0);
  test('Entity selector sorted alphabetically', isSorted, firstGroupItems.slice(0, 3).join(', '));

  // === Test 3: Entity Timeline ===
  console.log('\n=== Test 3: Entity Timeline ===');
  await page.evaluate(() => {
    const sel = document.getElementById('pn-entity-select');
    sel.value = 'Sword';
    sel.dispatchEvent(new Event('change'));
  });
  await wait(500);

  const swordContent = await page.evaluate(() =>
    document.getElementById('pn-entity-content').innerHTML.length
  );
  test('Sword timeline renders', swordContent > 100, `${swordContent} chars`);

  const swordPatches = await page.evaluate(() =>
    document.querySelectorAll('#pn-entity-content table tbody tr').length
  );
  test('Sword has patch rows', swordPatches >= 5, `${swordPatches} rows`);

  // === Test 4: Re-render duplication ===
  console.log('\n=== Test 4: Re-render duplication ===');
  await page.evaluate(() => {
    if (typeof navigateToPatchEntity === 'function') navigateToPatchEntity('CL-40');
  });
  await wait(800);

  const pillCountAfter = await page.evaluate(() =>
    document.querySelectorAll('#pn-season-pills .pill').length
  );
  test('No duplicate pills after re-render', pillCountAfter === 9, `got ${pillCountAfter}`);

  const optgroupCountAfter = await page.evaluate(() =>
    document.querySelectorAll('#pn-entity-select optgroup').length
  );
  test('No duplicate optgroups after re-render', optgroupCountAfter === optgroupCount,
    `before=${optgroupCount}, after=${optgroupCountAfter}`);

  // === Test 5: Season pill click ===
  console.log('\n=== Test 5: Season pill click ===');
  await page.evaluate(() => {
    document.querySelectorAll('#pn-season-pills .pill')[0].click();
  });
  await wait(300);

  const s1Active = await page.evaluate(() => {
    const a = document.querySelector('#pn-season-pills .pill.active');
    return a ? a.textContent : null;
  });
  test('S1 pill activates on click', s1Active === 'S1', `got ${s1Active}`);

  const s1Header = await page.evaluate(() => {
    const h = document.getElementById('pn-content').querySelector('h2');
    return h ? h.textContent : null;
  });
  test('S1 content shows Season 1', s1Header && s1Header.includes('Season 1'), `got "${s1Header}"`);

  // === Test 6: Impact filter controls ===
  console.log('\n=== Test 6: Impact filter ===');

  await page.evaluate(() => {
    const sel = document.getElementById('pn-impact-filter');
    sel.value = '';
    sel.dispatchEvent(new Event('change'));
  });
  await wait(200);

  const allVisible = await page.evaluate(() => {
    const rows = document.querySelectorAll('#pn-impact-table tbody tr');
    let v = 0; rows.forEach(r => { if (r.style.display !== 'none') v++; }); return v;
  });
  test('All seasons shows more rows', allVisible > visibleImpactRows, `S9=${visibleImpactRows}, all=${allVisible}`);

  await page.evaluate(() => {
    const cb = document.getElementById('pn-impact-showall');
    cb.checked = true;
    cb.dispatchEvent(new Event('change'));
  });
  await wait(200);

  const showAllVisible = await page.evaluate(() => {
    const rows = document.querySelectorAll('#pn-impact-table tbody tr');
    let v = 0; rows.forEach(r => { if (r.style.display !== 'none') v++; }); return v;
  });
  test('Show all reveals more rows', showAllVisible > allVisible, `filtered=${allVisible}, all=${showAllVisible}`);

  // === Test 7: Other pages ===
  console.log('\n=== Test 7: Key pages ===');

  await page.evaluate(() => { window.location.hash = '#review-bombing'; });
  await wait(1000);
  const rbContent = await page.evaluate(() =>
    document.getElementById('page-review-bombing').innerHTML.length
  );
  test('Review Bombing has content', rbContent > 500, `${rbContent} chars`);

  await page.evaluate(() => { window.location.hash = '#entity-tracker'; });
  await wait(500);
  const etContent = await page.evaluate(() =>
    document.getElementById('page-entity-tracker').innerHTML.length
  );
  test('Entity Tracker has content', etContent > 500, `${etContent} chars`);

  // === Test 8: Methodology + About ===
  console.log('\n=== Test 8: Methodology & About ===');

  await page.evaluate(() => { window.location.hash = '#methodology'; });
  await wait(500);
  const methText = await page.evaluate(() =>
    document.getElementById('page-methodology').textContent
  );
  test('Methodology mentions patches', methText.includes('patches'));
  test('Methodology mentions pipeline', methText.includes('Pipeline') || methText.includes('pipeline'));

  await page.evaluate(() => { window.location.hash = '#about'; });
  await wait(500);
  const aboutText = await page.evaluate(() =>
    document.getElementById('page-about').textContent
  );
  test('About mentions 247,453 reviews', aboutText.includes('247,453'));

  // === Test 9: Deep link ===
  console.log('\n=== Test 9: Deep link ===');
  await page.goto(BASE + '/#patch-notes?entity=CL-40', { waitUntil: 'networkidle0', timeout: 30000 });
  await wait(1500);

  const dlEntity = await page.evaluate(() =>
    document.getElementById('pn-entity-select').value
  );
  test('Deep link entity=CL-40 sets selector', dlEntity === 'CL-40', `got "${dlEntity}"`);

  // === Summary ===
  console.log('\n' + '='.repeat(50));
  console.log(`RESULTS: ${passedTests}/${totalTests} passed`);

  if (errors.length) {
    console.log(`\nJS ERRORS (${errors.length}):`);
    errors.forEach(e => console.log(`  ${e}`));
  }
  if (failures.length) {
    console.log(`\nFAILURES (${failures.length}):`);
    failures.forEach(f => console.log(`  ${f}`));
  }
  if (!errors.length && !failures.length) {
    console.log('\nALL TESTS PASSED!');
  }

  await browser.close();
  process.exit(failures.length > 0 || errors.length > 0 ? 1 : 0);
})();
