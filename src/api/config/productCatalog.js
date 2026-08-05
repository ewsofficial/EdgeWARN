import catalog from './product-catalog.json' with { type: 'json' };

const SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

function assertCatalog(entries) {
  const ids = new Set();
  const legacyIds = new Set();
  const directories = new Set();
  for (const entry of entries) {
    if (!entry || !SLUG.test(entry.id) || !entry.legacyId || !entry.storageDirectory || !entry.legacyFilePrefix) {
      throw new Error('Invalid render product catalog entry');
    }
    for (const [set, value, field] of [[ids, entry.id, 'id'], [legacyIds, entry.legacyId, 'legacyId'], [directories, entry.storageDirectory, 'storageDirectory']]) {
      if (set.has(value)) throw new Error(`Duplicate render catalog ${field}: ${value}`);
      set.add(value);
    }
  }
  return Object.freeze(entries.map((entry) => Object.freeze({ ...entry })));
}

export const productCatalog = assertCatalog(catalog);
export const productById = new Map(productCatalog.map((product) => [product.id, product]));
export const productByLegacyId = new Map(productCatalog.map((product) => [product.legacyId, product]));

export function getProductByLegacyId(legacyId) {
  return productByLegacyId.get(legacyId) || null;
}
