import os
import requests
import time

JIMSPORTS_API_KEY = os.environ['JIMSPORTS_API_KEY']
SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = 'xqksc3-ua.myshopify.com'
API_VERSION = '2025-10'
SYNC_LIMIT = int(os.environ.get('SYNC_LIMIT', '0'))

SHOPIFY_BASE = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}'
HEADERS_JIM = {
    'ClientAuth': JIMSPORTS_API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'JimSports-Shopify-Sync/1.0'
}
HEADERS_SHOPIFY = {
    'X-Shopify-Access-Token': SHOPIFY_TOKEN,
    'Content-Type': 'application/json'
}


def shopify_request(method, endpoint, data=None, retries=5):
    url = endpoint if endpoint.startswith('http') else f'{SHOPIFY_BASE}/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=HEADERS_SHOPIFY, json=data, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 10))
                print(f'  Rate limit Shopify, esperando {wait}s...')
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                print(f'  Shopify {method} HTTP {r.status_code}: {r.text[:200]}')
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


def get_location_id():
    r = shopify_request('GET', 'shop.json')
    if not r:
        return None
    return r.json().get('shop', {}).get('primary_location_id')


def fetch_existing_by_sku():
    existing = {}
    url = f'{SHOPIFY_BASE}/products.json?limit=250&fields=id,variants,tags'
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
                        'variant_id': v['id'],
                        'inventory_item_id': v.get('inventory_item_id')
                    }
        url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                url = part.split(';')[0].strip().strip('<>')
                break
    return existing


def set_inventory(inventory_item_id, location_id, quantity):
    return shopify_request('POST', 'inventory_levels/set.json', data={
        'location_id': location_id,
        'inventory_item_id': inventory_item_id,
        'available': int(quantity)
    })


def fetch_brands():
    data = jim_request('brands')
    return {str(b['id']): b.get('name', '') for b in (data or [])}


def pick_ean(product):
    variants = product.get('variants') or []
    for v in variants:
        if v.get('default') and not v.get('discontinued') and v.get('ean13'):
            return v['ean13']
    for v in variants:
        if not v.get('discontinued') and v.get('ean13'):
            return v['ean13']
    return product.get('ean13') or None


def pick_stock(product):
    variants = product.get('variants') or []
    if variants:
        total = 0
        for v in variants:
            if not v.get('discontinued'):
                try:
                    total += int(v.get('stock') or 0)
                except (TypeError, ValueError):
                    pass
        return total
    try:
        return int(product.get('stock') or 0)
    except (TypeError, ValueError):
        return 0


def sync():
    print('=== Sincronizando JimSports -> Shopify ===')

    location_id = get_location_id()
    if not location_id:
        print('ERROR: no se encontró location en Shopify')
        return
    print(f'Location id: {location_id}')

    brands = fetch_brands()
    print(f'{len(brands)} marcas cargadas')

    existing = fetch_existing_by_sku()
    print(f'{len(existing)} productos JimSports ya en Shopify')

    jim_ids = jim_request('products')
    if not jim_ids:
        print('ERROR: no se pudieron obtener productos de Jim Sports')
        return
    if SYNC_LIMIT:
        jim_ids = jim_ids[:SYNC_LIMIT]
    total = len(jim_ids)
    print(f'{total} productos a procesar\n')

    created = updated = no_ean = disc = errors = 0

    for i, jim_id in enumerate(jim_ids, 1):
        jim_id = str(jim_id)
        product = jim_request(f'product/{jim_id}')
        if not product:
            errors += 1
            continue

        if product.get('discontinued'):
            disc += 1
            continue

        ean = pick_ean(product)
        if not ean:
            no_ean += 1
            print(f'  [{i}/{total}] {jim_id}: sin EAN, skip')
            continue

        name_obj = product.get('name') or {}
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'
        desc_obj = product.get('description') or {}
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''

        price = str(product.get('price') or '0.00')
        stock = pick_stock(product)

        brand = brands.get(str(product.get('brand_id', '')), '')
        brand_tag = brand.lower().replace(' ', '-') if brand else 'sin-marca'
        cat_tags = [f'cat-{c}' for c in (product.get('category_ids') or [])]
        tags = ','.join(['jimsports', f'marca-{brand_tag}'] + cat_tags)

        images = [{'src': url} for url in (product.get('images') or []) if url]

        if ean in existing:
            info = existing[ean]
            shopify_request('PUT', f'variants/{info["variant_id"]}.json', data={
                'variant': {'id': info['variant_id'], 'price': price}
            })
            if info.get('inventory_item_id'):
                set_inventory(info['inventory_item_id'], location_id, stock)
            updated += 1
            print(f'  [{i}/{total}] {ean} {name[:50]} -> actualizado (stock {stock})')
        else:
            r = shopify_request('POST', 'products.json', data={
                'product': {
                    'title': name,
                    'body_html': desc,
                    'handle': f'jimsports-{ean}',
                    'vendor': brand or 'Jim Sports',
                    'tags': tags,
                    'images': images,
                    'variants': [{
                        'sku': ean,
                        'barcode': ean,
                        'price': price,
                        'inventory_management': 'shopify',
                    }]
                }
            })
            if r and r.json().get('product'):
                v = (r.json()['product'].get('variants') or [{}])[0]
                if v.get('inventory_item_id'):
                    set_inventory(v['inventory_item_id'], location_id, stock)
                created += 1
                print(f'  [{i}/{total}] {ean} {name[:50]} -> CREADO (stock {stock})')
            else:
                errors += 1

        time.sleep(0.4)

    print(f'\n=== RESUMEN ===')
    print(f'Creados:        {created}')
    print(f'Actualizados:   {updated}')
    print(f'Sin EAN:        {no_ean}')
    print(f'Discontinuados: {disc}')
    print(f'Errores:        {errors}')


if __name__ == '__main__':
    sync()
    print('Sync completado')
