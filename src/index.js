const fs = require('node:fs/promises');
const path = require('node:path');

const ExcelJS = require('exceljs');
const { chromium } = require('playwright-core');

const DEFAULT_QUERY = 'пальто из натуральной шерсти';
const DEFAULT_DEST = '-1257786';
const DEFAULT_OUTPUT_DIR = path.resolve(process.cwd(), 'output');
const DEFAULT_BATCH_SIZE = 40;
const DEFAULT_CARD_CONCURRENCY = 8;
const DEFAULT_TIMEOUT_MS = 60_000;

const EDGE_CANDIDATES = [
  'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  path.join(process.env.LOCALAPPDATA || '', 'Google\\Chrome\\Application\\chrome.exe'),
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunk(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function parseBoolean(value, fallback) {
  if (value === undefined || value === null) {
    return fallback;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }

  return fallback;
}

function parseNumber(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseArgs(argv) {
  const options = {
    query: DEFAULT_QUERY,
    dest: DEFAULT_DEST,
    outputDir: DEFAULT_OUTPUT_DIR,
    headless: false,
    batchSize: DEFAULT_BATCH_SIZE,
    cardConcurrency: DEFAULT_CARD_CONCURRENCY,
    limit: 0,
    timeoutMs: DEFAULT_TIMEOUT_MS,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const raw = argv[index];
    if (!raw.startsWith('--')) {
      continue;
    }

    const [keyPart, inlineValue] = raw.split('=');
    const key = keyPart.slice(2);
    const next = inlineValue !== undefined ? inlineValue : argv[index + 1];
    const hasNextValue = inlineValue !== undefined || (next && !next.startsWith('--'));
    const value = hasNextValue ? next : undefined;

    switch (key) {
      case 'query':
        if (value) {
          options.query = value;
        }
        break;
      case 'dest':
        if (value) {
          options.dest = value;
        }
        break;
      case 'output-dir':
        if (value) {
          options.outputDir = path.resolve(value);
        }
        break;
      case 'headless':
        options.headless = parseBoolean(value, true);
        break;
      case 'batch-size':
        options.batchSize = parseNumber(value, DEFAULT_BATCH_SIZE);
        break;
      case 'card-concurrency':
        options.cardConcurrency = parseNumber(value, DEFAULT_CARD_CONCURRENCY);
        break;
      case 'limit':
        options.limit = parseNumber(value, 0);
        break;
      case 'timeout-ms':
        options.timeoutMs = parseNumber(value, DEFAULT_TIMEOUT_MS);
        break;
      default:
        break;
    }

    if (hasNextValue && inlineValue === undefined) {
      index += 1;
    }
  }

  return options;
}

function formatDuration(milliseconds) {
  const totalSeconds = Math.round(milliseconds / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
}

function normalizeWhitespace(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
}

function unique(values) {
  return [...new Set(values)];
}

function volumeFromNmId(nmId) {
  return Math.floor(Number(nmId) / 100000);
}

function partFromNmId(nmId) {
  return Math.floor(Number(nmId) / 1000);
}

function buildProductUrl(nmId) {
  return `https://www.wildberries.ru/catalog/${nmId}/detail.aspx`;
}

function buildSellerUrl(supplierId) {
  return supplierId ? `https://www.wildberries.ru/seller/${supplierId}` : '';
}

function buildSearchReferer(query) {
  return `https://www.wildberries.ru/catalog/0/search.aspx?search=${encodeURIComponent(query)}`;
}

function safeNumber(value) {
  return Number.isFinite(value) ? value : null;
}

function getDistinctSizes(product) {
  const sizes = Array.isArray(product?.sizes) ? product.sizes : [];
  return unique(
    sizes
      .map((size) => normalizeWhitespace(size?.origName || size?.name))
      .filter(Boolean),
  );
}

function getTotalStock(product) {
  const sizes = Array.isArray(product?.sizes) ? product.sizes : [];
  const summed = sizes.reduce((total, size) => {
    const stocks = Array.isArray(size?.stocks) ? size.stocks : [];
    return total + stocks.reduce((subtotal, stock) => subtotal + (Number(stock?.qty) || 0), 0);
  }, 0);

  if (summed > 0) {
    return summed;
  }

  return Number(product?.totalQuantity) || 0;
}

function getProductPrice(product) {
  const prices = (Array.isArray(product?.sizes) ? product.sizes : [])
    .map((size) => Number(size?.price?.product))
    .filter((value) => Number.isFinite(value) && value > 0);

  if (prices.length === 0) {
    return null;
  }

  return Number((Math.min(...prices) / 100).toFixed(2));
}

function getGroupedOptions(cardInfo) {
  if (Array.isArray(cardInfo?.grouped_options) && cardInfo.grouped_options.length > 0) {
    return cardInfo.grouped_options;
  }

  if (Array.isArray(cardInfo?.options) && cardInfo.options.length > 0) {
    return [
      {
        group_name: 'Характеристики',
        options: cardInfo.options,
      },
    ];
  }

  return [];
}

function findCharacteristic(groupedOptions, name) {
  const expected = normalizeWhitespace(name).toLowerCase();
  for (const group of groupedOptions) {
    for (const option of group?.options || []) {
      const optionName = normalizeWhitespace(option?.name).toLowerCase();
      if (optionName === expected) {
        return normalizeWhitespace(option?.value);
      }
    }
  }

  return '';
}

function stringifyCharacteristics(groupedOptions) {
  return JSON.stringify(groupedOptions, null, 2);
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);

  return results;
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function detectBrowserExecutable() {
  for (const candidate of EDGE_CANDIDATES) {
    if (candidate && (await fileExists(candidate))) {
      return candidate;
    }
  }

  throw new Error(
    'Не найден исполняемый файл Edge/Chrome. Проверьте стандартную установку браузера или добавьте путь в EDGE_CANDIDATES.',
  );
}

class WildberriesClient {
  constructor(options) {
    this.options = options;
    this.browser = null;
    this.context = null;
    this.page = null;
    this.mediaRouteHosts = [];
  }

  async init() {
    const executablePath = await detectBrowserExecutable();

    this.browser = await chromium.launch({
      headless: this.options.headless,
      executablePath,
      args: ['--disable-blink-features=AutomationControlled'],
    });

    this.context = await this.browser.newContext({
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
      locale: 'ru-RU',
      timezoneId: 'Europe/Moscow',
      viewport: { width: 1366, height: 900 },
      colorScheme: 'light',
    });

    await this.context.addInitScript(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
      Object.defineProperty(navigator, 'language', { get: () => 'ru-RU' });
      Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US', 'en'] });
    });

    this.page = await this.context.newPage();

    await this.ensureSession();
    this.mediaRouteHosts = await this.fetchMediaRouteHosts();
  }

  async close() {
    if (this.browser) {
      await this.browser.close();
    }
  }

  async ensureSession() {
    const startedAt = Date.now();
    await this.page.goto('https://www.wildberries.ru/', {
      waitUntil: 'domcontentloaded',
      timeout: this.options.timeoutMs,
    });

    while (Date.now() - startedAt < 120_000) {
      const title = await this.page.title().catch(() => '');
      const bodyText = await this.page.locator('body').innerText({ timeout: 1_500 }).catch(() => '');
      const blocked =
        title.includes('Почти готово') ||
        bodyText.includes('Подозрительная активность') ||
        bodyText.includes('Проверяем браузер') ||
        bodyText.includes('Что-то не так');

      if (!blocked) {
        return;
      }

      console.log('[session] Wildberries показывает антибот-страницу, жду прохождения проверки...');
      await this.page.waitForTimeout(5_000);
    }

    throw new Error(
      'Не удалось пройти антибот-проверку Wildberries за 120 секунд. Перезапустите скрипт с видимым браузером.',
    );
  }

  async requestJson(url, { description, headers = {}, allow404 = false } = {}) {
    let lastError = null;

    for (let attempt = 1; attempt <= 3; attempt += 1) {
      const response = await this.context.request.get(url, {
        headers,
        timeout: this.options.timeoutMs,
        failOnStatusCode: false,
      });

      if (response.status() >= 200 && response.status() < 300) {
        try {
          return await response.json();
        } catch (error) {
          lastError = new Error(
            `${description || 'request'} returned non-JSON response for ${url}: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }
      }

      if (allow404 && response.status() === 404) {
        return null;
      }

      const responseText = await response.text().catch(() => '');
      lastError = new Error(
        `${description || 'request'} failed with status ${response.status()} for ${url}${
          responseText ? `: ${responseText.slice(0, 180)}` : ''
        }`,
      );

      if ((response.status() === 403 || response.status() === 498) && attempt < 3) {
        console.log(`[retry] ${description || url}: статус ${response.status()}, обновляю браузерную сессию.`);
        await this.ensureSession();
        continue;
      }

      if (attempt < 3) {
        await sleep(attempt * 1_000);
      }
    }

    throw lastError;
  }

  buildSearchUrl(query, pageNumber) {
    const params = new URLSearchParams({
      ab_testing: 'false',
      appType: '1',
      curr: 'rub',
      dest: this.options.dest,
      hide_dflags: '131072',
      hide_dtype: '13',
      hide_vflags: '4294967296',
      inheritFilters: 'false',
      lang: 'ru',
      query,
      resultset: 'catalog',
      sort: 'popular',
      spp: '30',
      suppressSpellcheck: 'false',
    });

    if (pageNumber > 1) {
      params.set('page', String(pageNumber));
    }

    return `https://www.wildberries.ru/__internal/u-search/exactmatch/sng/common/v18/search?${params.toString()}`;
  }

  buildSearchHeaders(query) {
    return {
      referer: buildSearchReferer(query),
      'x-queryid': `qid${Date.now()}${Math.floor(Math.random() * 10_000)}`,
      'x-userid': '0',
    };
  }

  async fetchSearchPage(query, pageNumber) {
    return this.requestJson(this.buildSearchUrl(query, pageNumber), {
      description: `search page ${pageNumber}`,
      headers: this.buildSearchHeaders(query),
    });
  }

  buildDetailUrl(productIds) {
    const params = new URLSearchParams({
      appType: '1',
      curr: 'rub',
      dest: this.options.dest,
      spp: '30',
      hide_vflags: '4294967296',
      hide_dflags: '131072',
      hide_dtype: '13',
      ab_testing: 'false',
      lang: 'ru',
      nm: productIds.join(';'),
    });

    return `https://www.wildberries.ru/__internal/u-card/cards/v4/detail?${params.toString()}`;
  }

  async fetchDetailBatch(productIds, query) {
    return this.requestJson(this.buildDetailUrl(productIds), {
      description: `detail batch ${productIds[0]}...`,
      headers: {
        referer: buildSearchReferer(query),
      },
    });
  }

  async fetchMediaRouteHosts() {
    const upstreams = await this.requestJson(`https://cdn.wbbasket.ru/api/v3/upstreams?t=${Date.now()}`, {
      description: 'cdn upstreams',
    });

    const hosts = upstreams?.origin?.mediabasket_route_map?.[0]?.hosts;
    if (!Array.isArray(hosts) || hosts.length === 0) {
      throw new Error('Не удалось получить карту basket-хостов из CDN Wildberries.');
    }

    return hosts;
  }

  resolveMediaHost(nmId) {
    const volume = volumeFromNmId(nmId);
    const matched = this.mediaRouteHosts.find(
      (host) => volume >= host.vol_range_from && volume <= host.vol_range_to,
    );

    if (!matched?.host) {
      throw new Error(`Для товара ${nmId} не найден basket-хост (vol=${volume}).`);
    }

    return matched.host;
  }

  buildCardInfoUrl(nmId) {
    const volume = volumeFromNmId(nmId);
    const part = partFromNmId(nmId);
    const host = this.resolveMediaHost(nmId);
    return `https://${host}/vol${volume}/part${part}/${nmId}/info/ru/card.json`;
  }

  buildImageUrls(nmId, photoCount) {
    const count = Number(photoCount) || 0;
    if (count <= 0) {
      return [];
    }

    const volume = volumeFromNmId(nmId);
    const part = partFromNmId(nmId);
    const host = this.resolveMediaHost(nmId);

    return Array.from({ length: count }, (_, index) => {
      const imageIndex = index + 1;
      return `https://${host}/vol${volume}/part${part}/${nmId}/images/big/${imageIndex}.webp`;
    });
  }

  async fetchCardInfo(nmId) {
    return this.requestJson(this.buildCardInfoUrl(nmId), {
      description: `card.json ${nmId}`,
      allow404: true,
    });
  }
}

function buildCatalogRow(product, cardInfo, client) {
  const groupedOptions = getGroupedOptions(cardInfo);
  const rating = safeNumber(Number(product?.nmReviewRating ?? product?.reviewRating ?? product?.rating));
  const reviewCount = Number(product?.nmFeedbacks ?? product?.feedbacks ?? 0);
  const price = getProductPrice(product);
  const sizes = getDistinctSizes(product);
  const photoCount = Number(cardInfo?.media?.photo_count) || Number(product?.pics) || 0;
  const country = findCharacteristic(groupedOptions, 'Страна производства');
  const baseName = normalizeWhitespace(product?.name || cardInfo?.imt_name);
  const brand = normalizeWhitespace(product?.brand);
  const displayName = brand && baseName ? `${brand} / ${baseName}` : baseName || brand;

  return {
    productUrl: buildProductUrl(product.id),
    article: String(product.id),
    name: displayName,
    price,
    description: normalizeWhitespace(cardInfo?.description),
    imageUrls: client.buildImageUrls(product.id, photoCount).join(', '),
    characteristics: stringifyCharacteristics(groupedOptions),
    sellerName: normalizeWhitespace(product?.supplier),
    sellerUrl: buildSellerUrl(product?.supplierId),
    sizes: sizes.join(', '),
    stock: getTotalStock(product),
    rating,
    reviewCount,
    country,
  };
}

async function collectProductIds(client, query, limit) {
  const firstPage = await client.fetchSearchPage(query, 1);
  const total = Number(firstPage?.total) || 0;
  const pageSize = Array.isArray(firstPage?.products) ? firstPage.products.length : 100;
  const totalPages = pageSize > 0 ? Math.ceil(total / pageSize) : 0;
  const orderedIds = [];

  for (const product of firstPage?.products || []) {
    orderedIds.push(Number(product.id));
  }

  console.log(`[search] страница 1/${totalPages}, товаров найдено: ${total}.`);

  for (let pageNumber = 2; pageNumber <= totalPages; pageNumber += 1) {
    if (limit > 0 && orderedIds.length >= limit) {
      break;
    }

    const pageData = await client.fetchSearchPage(query, pageNumber);
    for (const product of pageData?.products || []) {
      orderedIds.push(Number(product.id));
    }

    console.log(`[search] страница ${pageNumber}/${totalPages}, накоплено товаров: ${orderedIds.length}.`);
  }

  return unique(orderedIds).slice(0, limit > 0 ? limit : undefined);
}

async function collectDetails(client, productIds, query, batchSize) {
  const batches = chunk(productIds, batchSize);
  const detailMap = new Map();

  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index];
    const response = await client.fetchDetailBatch(batch, query);
    const products = response?.products || [];

    for (const product of products) {
      detailMap.set(Number(product.id), product);
    }

    console.log(`[detail] батч ${index + 1}/${batches.length}, получено карточек: ${detailMap.size}.`);
  }

  return detailMap;
}

async function collectCardInfos(client, productIds, concurrency) {
  const results = await mapWithConcurrency(productIds, concurrency, async (productId, index) => {
    const cardInfo = await client.fetchCardInfo(productId);
    if ((index + 1) % 100 === 0 || index + 1 === productIds.length) {
      console.log(`[card.json] обработано ${index + 1}/${productIds.length}.`);
    }
    return [productId, cardInfo];
  });

  return new Map(results);
}

async function ensureOutputDir(outputDir) {
  await fs.mkdir(outputDir, { recursive: true });
}

async function writeWorkbook(filePath, rows, sheetName) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'Codex';
  workbook.created = new Date();
  workbook.modified = new Date();

  const worksheet = workbook.addWorksheet(sheetName);
  worksheet.columns = [
    { header: 'Ссылка на товар', key: 'productUrl', width: 28 },
    { header: 'Артикул', key: 'article', width: 14 },
    { header: 'Название', key: 'name', width: 38 },
    { header: 'Цена', key: 'price', width: 12 },
    { header: 'Описание', key: 'description', width: 56 },
    { header: 'Ссылки на изображения', key: 'imageUrls', width: 72 },
    { header: 'Все характеристики', key: 'characteristics', width: 60 },
    { header: 'Название селлера', key: 'sellerName', width: 32 },
    { header: 'Ссылка на селлера', key: 'sellerUrl', width: 28 },
    { header: 'Размеры товара', key: 'sizes', width: 24 },
    { header: 'Остатки по товару', key: 'stock', width: 18 },
    { header: 'Рейтинг', key: 'rating', width: 12 },
    { header: 'Количество отзывов', key: 'reviewCount', width: 18 },
  ];

  worksheet.addRows(
    rows.map((row) => ({
      productUrl: row.productUrl,
      article: row.article,
      name: row.name,
      price: row.price,
      description: row.description,
      imageUrls: row.imageUrls,
      characteristics: row.characteristics,
      sellerName: row.sellerName,
      sellerUrl: row.sellerUrl,
      sizes: row.sizes,
      stock: row.stock,
      rating: row.rating,
      reviewCount: row.reviewCount,
    })),
  );

  worksheet.views = [{ state: 'frozen', ySplit: 1 }];
  worksheet.autoFilter = {
    from: 'A1',
    to: 'M1',
  };

  const headerRow = worksheet.getRow(1);
  headerRow.font = { bold: true };
  headerRow.alignment = { vertical: 'middle', wrapText: true };
  headerRow.height = 24;

  for (let rowIndex = 2; rowIndex <= worksheet.rowCount; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    row.alignment = { vertical: 'top' };
    row.getCell('A').font = { color: { argb: 'FF0563C1' }, underline: true };
    row.getCell('I').font = { color: { argb: 'FF0563C1' }, underline: true };
    row.getCell('D').numFmt = '#,##0.00';
    row.getCell('E').alignment = { vertical: 'top', wrapText: true };
    row.getCell('F').alignment = { vertical: 'top', wrapText: true };
    row.getCell('G').alignment = { vertical: 'top', wrapText: true };
    row.getCell('H').alignment = { vertical: 'top', wrapText: true };
    row.getCell('I').alignment = { vertical: 'top', wrapText: true };
    row.getCell('J').alignment = { vertical: 'top', wrapText: true };
  }

  await workbook.xlsx.writeFile(filePath);
}

async function main() {
  const startedAt = Date.now();
  const options = parseArgs(process.argv.slice(2));

  await ensureOutputDir(options.outputDir);

  console.log(
    `[start] Запуск выгрузки Wildberries по запросу "${options.query}" (dest=${options.dest}, headless=${options.headless}).`,
  );

  const client = new WildberriesClient(options);

  try {
    await client.init();

    const productIds = await collectProductIds(client, options.query, options.limit);
    console.log(`[search] итоговое количество товаров к обработке: ${productIds.length}.`);

    const detailMap = await collectDetails(client, productIds, options.query, options.batchSize);
    const cardInfoMap = await collectCardInfos(client, productIds, options.cardConcurrency);

    const rows = productIds
      .map((productId) => {
        const product = detailMap.get(productId);
        if (!product) {
          return null;
        }

        const cardInfo = cardInfoMap.get(productId);
        return buildCatalogRow(product, cardInfo, client);
      })
      .filter(Boolean);

    const filteredRows = rows.filter((row) => {
      if (!Number.isFinite(row.rating) || !Number.isFinite(row.price)) {
        return false;
      }

      const rating = Number(row.rating);
      const price = Number(row.price);
      const country = normalizeWhitespace(row.country).toLowerCase();
      return rating >= 4.5 && price <= 10000 && country.includes('россия');
    });

    const fullCatalogPath = path.join(options.outputDir, 'wildberries_catalog_full.xlsx');
    const filteredCatalogPath = path.join(options.outputDir, 'wildberries_catalog_filtered.xlsx');

    await writeWorkbook(fullCatalogPath, rows, 'Каталог');
    await writeWorkbook(filteredCatalogPath, filteredRows, 'Фильтр');

    console.log(`[done] Полный каталог: ${fullCatalogPath}`);
    console.log(`[done] Фильтрованный каталог: ${filteredCatalogPath}`);
    console.log(`[done] Всего строк: ${rows.length}, после фильтра: ${filteredRows.length}.`);
    console.log(`[done] Время выполнения: ${formatDuration(Date.now() - startedAt)}.`);
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error('[error]', error instanceof Error ? error.stack || error.message : error);
  process.exitCode = 1;
});
