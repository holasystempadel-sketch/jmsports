"""
JimSports -> Shopify sync v2.

Canvis respecte al v1:
  - Multiplicador de preu (per defecte x2) per convertir el `price` de l'API
    (preu de coste B2B) al PVP final (igual que jimsports.shop).
  - Crea variants quan el producte de Jim Sports té múltiples references
    (ex. tèxtil amb talles/colors). Cada variant té el seu propi EAN, preu i stock.
  - Llegeix `/v1/attribute_values` i `/v1/attributes` per traduir els IDs
    numèrics de la referencia (ex. ".028.32") a etiquetes ("AZUL / S").
  - Crea col·lecció automàtica per marca (vendor) — Smart Collection que
    agrupa per tag `marca-<slug>`.
  - Tags `outlet` / `novedad` segons els flags del producte de l'API.
  - Mode SYNC_LIMIT per fer proves amb pocs productes (per defecte 0 = tots).
  - DEBUG_REF=A000971 per imprimir el JSON sencer d'una referència concreta.

Variables d'entorn:
  JIMSPORTS_API_KEY  (secret) — clau ClientAuth
  SHOPIFY_TOKEN      (secret) — Admin API Access Token
  SHOPIFY_STORE      (defecte 'xqksc3-ua.myshopify.com')
  PRICE_MULTIPLIER   (defecte 2.0)
  SYNC_LIMIT         (defecte 0 = tots)
  DEBUG_REF          (opcional, ex. 'A000971')
"""
import os
import re
import json
import time
import requests

JIMSPORTS_API_KEY = os.environ['JIMSPORTS_API_KEY']
SHOPIFY_TOKEN     = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE     = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API_VERSION       = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
SYNC_LIMIT        = int(os.environ.get('SYNC_LIMIT', '0'))
PRICE_MULTIPLIER  = float(os.environ.get('PRICE_MULTIPLIER', '2.0'))
DEBUG_REF         = os.environ.get('DEBUG_REF', '').strip()

SHOPIFY_BASE = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}'

HEADERS_JIM = {
    'ClientAuth': JIMSPORTS_API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'JimSports-Shopify-Sync/2.0',
}
HEADERS_SHOPIFY = {
    'X-Shopify-Access-Token': SHOPIFY_TOKEN,
    'Content-Type': 'application/json',
}

# ─── HTTP HELPERS ─────────────────────────────────────────────────────────────

def shopify_request(method, endpoint, data=None, retries=5):
    url = endpoint if endpoint.startswith('http') else f'{SHOPIFY_BASE}/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=HEADERS_SHOPIFY, json=data, timeout=30)
            if r.status_code == 429:
                wait = float(r.headers.get('Retry-After', 10))
                print(f'  Rate limit Shopify, esperant {wait}s')
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                print(f'  Shopify {method} {endpoint} HTTP {r.status_code}: {r.text[:300]}')
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(5)
            else:
                print(f'  FALLO Shopify {method} {endpoint}: {e}')
                return None
    return None


def jim_request(endpoint, retries=5):
    url = f'https://api.jimsports.com/v1/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS_JIM, timeout=30)
            if r.status_code == 429:
                time.sleep(10)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(3)
            else:
                print(f'  FALLO Jim {endpoint}: {e}')
                return None
    return None


# ─── PREU ─────────────────────────────────────────────────────────────────────

def pvp(raw):
    try:
        return f'{round(float(raw or 0) * PRICE_MULTIPLIER, 2):.2f}'
    except (TypeError, ValueError):
        return '0.00'


# ─── MAPPINGS GLOBALS ─────────────────────────────────────────────────────────

def fetch_brand_map():
    data = jim_request('brands') or []
    return {b['id']: (b.get('name') or '').strip() for b in data}


def fetch_attribute_value_label():
    """Retorna {attribute_value_id: 'NOM'} en castellà."""
    data = jim_request('attribute_values') or []
    out = {}
    for av in data:
        name_obj = av.get('name') or {}
        out[av['id']] = (
            name_obj.get('es-ES') or name_obj.get('en-US') or str(av['id'])
        )
    return out


# ─── PRODUCTES SHOPIFY EXISTENTS ──────────────────────────────────────────────

def fetch_existing():
    """Retorna {ean: {product_id, variant_id, inventory_item_id}} dels productes amb tag jimsports."""
    existing = {}
    url = f'{SHOPIFY_BASE}/products.json?limit=250&fields=id,handle,tags,variants'
    while url:
        r = shopify_request('GET', url)
        if not r:
            break
        for p in r.json().get('products', []):
            tags = (p.get('tags') or '').lower()
            if 'jimsports' not in tags:
                continue
            for v in p.get('variants', []):
                sku = (v.get('sku') or '').strip()
                if sku:
                    existing[sku] = {
                        'product_id': p['id'],
                        'variant_id': v['id'],
                        'inventory_item_id': v.get('inventory_item_id'),
                    }
        url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                url = part.split(';')[0].strip().strip('<>')
                break
    return existing


# ─── INVENTARI ────────────────────────────────────────────────────────────────

def get_location_id():
    r = shopify_request('GET', 'shop.json')
    if not r:
        return None
    return r.json().get('shop', {}).get('primary_location_id')


def set_inventory(inventory_item_id, location_id, quantity):
    return shopify_request('POST', 'inventory_levels/set.json', data={
        'location_id': location_id,
        'inventory_item_id': inventory_item_id,
        'available': int(quantity or 0),
    })


# ─── VARIANTS ─────────────────────────────────────────────────────────────────

def variant_label_from_reference(ref, base_ref, attr_value_label):
    """
    A partir de la referència 'A000971.028.32' i la base 'A000971',
    retorna 'AZUL / S' si els IDs estan a attr_value_label, sino la ref completa.
    """
    if not ref.startswith(base_ref):
        return ref
    suffix = ref[len(base_ref):].lstrip('.')
    if not suffix:
        return ref
    parts = re.split(r'[\.\-_]', suffix)
    labels = []
    for p in parts:
        if not p:
            continue
        try:
            avid = int(p)
            label = attr_value_label.get(avid)
            labels.append(label or p)
        except ValueError:
            labels.append(p)
    return ' / '.join(labels) if labels else ref


def build_variants(product, attr_value_label):
    """Construeix la llista de variants a passar a Shopify."""
    base_ref = product.get('reference', '')
    raw = [v for v in (product.get('variants') or []) if not v.get('discontinued')]

    # Cas A: 0 o 1 variant → producte simple
    if len(raw) <= 1:
        v = raw[0] if raw else product
        ean = v.get('ean13') or product.get('ean13')
        if not ean:
            return None, 'no-ean'
        return [{
            'sku': ean,
            'barcode': ean,
            'price': pvp(v.get('price') or product.get('price')),
            'inventory_management': 'shopify',
            '_stock': int(v.get('stock') or product.get('stock') or 0),
        }], None

    # Cas B: múltiples variants → opció "Variante"
    out = []
    seen_labels = set()
    for v in raw:
        ean = v.get('ean13')
        if not ean:
            continue
        label = variant_label_from_reference(v.get('reference', ''), base_ref, attr_value_label)
        # Garantir unicitat (Shopify no permet labels duplicats)
        original = label
        idx = 2
        while label in seen_labels:
            label = f'{original} ({idx})'
            idx += 1
        seen_labels.add(label)
        out.append({
            'sku': ean,
            'barcode': ean,
            'option1': label,
            'price': pvp(v.get('price') or product.get('price')),
            'inventory_management': 'shopify',
            '_stock': int(v.get('stock') or 0),
        })
    if not out:
        return None, 'no-ean'
    return out, None


# ─── COL·LECCIONS PER MARCA ───────────────────────────────────────────────────

def ensure_brand_collection(brand_name, cache):
    """Crea (si no existeix) una Smart Collection per marca i la torna a la cache."""
    if not brand_name:
        return None
    if brand_name in cache:
        return cache[brand_name]
    slug = re.sub(r'[^a-z0-9]+', '-', brand_name.lower()).strip('-')
    handle = f'marca-{slug}'
    # Comprovar si ja existeix
    r = shopify_request('GET', f'smart_collections.json?handle={handle}')
    if r:
        cols = r.json().get('smart_collections', [])
        if cols:
            cache[brand_name] = cols[0]['id']
            return cache[brand_name]
    # Crear
    r = shopify_request('POST', 'smart_collections.json', data={
        'smart_collection': {
            'title': brand_name,
            'handle': handle,
            'published': True,
            'rules': [{'column': 'tag', 'relation': 'equals', 'condition': handle}],
            'disjunctive': False,
        },
    })
    if r:
        cid = r.json().get('smart_collection', {}).get('id')
        cache[brand_name] = cid
        return cid
    return None


# ─── TAGS ─────────────────────────────────────────────────────────────────────

def build_tags(product, brand):
    brand_slug = re.sub(r'[^a-z0-9]+', '-', brand.lower()).strip('-') if brand else 'sin-marca'
    cats = [f'cat-{c}' for c in (product.get('category_ids') or [])]
    extras = []
    if product.get('new'):     extras.append('novedad')
    if product.get('outlet'):  extras.append('outlet')
    return ','.join(['jimsports', f'marca-{brand_slug}'] + cats + extras)


# ─── CICLE PRINCIPAL ──────────────────────────────────────────────────────────

def sync():
    print('=== JimSports -> Shopify (v2) ===')
    print(f'Store: {SHOPIFY_STORE}  ·  Multiplicador: x{PRICE_MULTIPLIER}  ·  Limit: {SYNC_LIMIT or "TOTS"}')

    location_id = get_location_id()
    if not location_id:
        print('ERROR: no es troba location a Shopify')
        return
    print(f'Location id: {location_id}')

    brands_map = fetch_brand_map()
    print(f'{len(brands_map)} marques carregades')

    attr_value_label = fetch_attribute_value_label()
    print(f'{len(attr_value_label)} valors d\'atribut carregats (Talla, Color, ...)')

    existing = fetch_existing()
    print(f'{len(existing)} variants existents a Shopify amb tag jimsports')

    jim_ids = jim_request('products') or []
    if SYNC_LIMIT:
        jim_ids = jim_ids[:SYNC_LIMIT]
    total = len(jim_ids)
    print(f'{total} productes a processar\n')

    brand_collection_cache = {}
    created = updated = skipped = errors = 0

    for i, jim_id in enumerate(jim_ids, 1):
        jim_id = str(jim_id)
        product = jim_request(f'product/{jim_id}')
        if not product:
            errors += 1
            continue

        ref = product.get('reference', '')
        if DEBUG_REF and ref == DEBUG_REF:
            print(f'\n=== DEBUG_REF {ref} JSON ===')
            print(json.dumps(product, indent=2, ensure_ascii=False)[:5000])
            print('=== /DEBUG ===\n')

        if product.get('discontinued'):
            skipped += 1
            continue

        name_obj = product.get('name') or {}
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'
        desc_obj = product.get('description') or {}
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''
        brand = brands_map.get(product.get('brand_id'), '')
        tags = build_tags(product, brand)
        images = [{'src': u} for u in (product.get('images') or []) if u]

        variants, err = build_variants(product, attr_value_label)
        if err == 'no-ean':
            skipped += 1
            print(f'  [{i}/{total}] {ref} SENSE EAN, skip')
            continue

        has_variants = len(variants) > 1

        # Construïm el payload Shopify
        product_payload = {
            'title': name,
            'body_html': desc,
            'handle': f'jimsports-{ref}',
            'vendor': brand or 'Jim Sports',
            'tags': tags,
            'images': images,
        }
        if has_variants:
            product_payload['options'] = [{'name': 'Variante'}]
        product_payload['variants'] = [
            {k: v for k, v in vd.items() if not k.startswith('_')}
            for vd in variants
        ]

        # Decidir si crear o actualitzar (per primer EAN del payload)
        primary_sku = variants[0]['sku']
        info = existing.get(primary_sku)

        if info:
            # Actualitzem només els camps que canvien fàcilment (tags, vendor, preus, stock)
            shopify_request('PUT', f'products/{info["product_id"]}.json', data={
                'product': {
                    'id': info['product_id'],
                    'tags': tags,
                    'vendor': brand or 'Jim Sports',
                },
            })
            # Per a productes simples actualitzem variant amb el SKU primary
            shopify_request('PUT', f'variants/{info["variant_id"]}.json', data={
                'variant': {
                    'id': info['variant_id'],
                    'price': variants[0]['price'],
                },
            })
            if info.get('inventory_item_id'):
                set_inventory(info['inventory_item_id'], location_id, variants[0]['_stock'])
            updated += 1
            print(f'  [{i}/{total}] {ref} {name[:40]} -> actualitzat')
        else:
            r = shopify_request('POST', 'products.json', data={'product': product_payload})
            if r and r.json().get('product'):
                created_p = r.json()['product']
                new_variants = created_p.get('variants', [])
                for sv, payload_v in zip(new_variants, variants):
                    if sv.get('inventory_item_id'):
                        set_inventory(sv['inventory_item_id'], location_id, payload_v['_stock'])
                created += 1
                print(f'  [{i}/{total}] {ref} {name[:40]} -> CREAT ({len(new_variants)} variants)')
            else:
                errors += 1

        # Assignar a col·lecció de marca
        if brand and not info:
            ensure_brand_collection(brand, brand_collection_cache)

        time.sleep(0.4)

    print('\n=== RESUM ===')
    print(f'Creats:        {created}')
    print(f'Actualitzats:  {updated}')
    print(f'Saltats:       {skipped}')
    print(f'Errors:        {errors}')


if __name__ == '__main__':
    sync()
    print('Sync v2 acabat')
