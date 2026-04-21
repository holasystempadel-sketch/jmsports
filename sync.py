import os
import requests
import time

JIMSPORTS_API_KEY = os.environ['JIMSPORTS_API_KEY']
SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = 'xqksc3-ua.myshopify.com'
API_VERSION = '2025-10'
SYNC_LIMIT = int(os.environ.get('SYNC_LIMIT', '0'))  # 0 = sin límite; útil para pruebas

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
                print(f'Rate limit Shopify, esperando {wait}s...')
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'Error Shopify intento {attempt+1}: {e}')
                time.sleep(5)
            else:
                print(f'Fallo definitivo Shopify {method} {endpoint}: {e}')
                return None
    return None


def jim_request(endpoint, retries=5):
    url = f'https://api.jimsports.com/v1/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS_JIM, timeout=30)
            if r.status_code == 429:
                print('Rate limit Jim Sports, esperando 10s...')
                time.sleep(10)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'Error Jim intento {attempt+1}: {e}')
                time.sleep(5)
            else:
                print(f'Fallo definitivo Jim {endpoint}: {e}')
                return None
    return None


def jim_get_images(product_id):
    data = jim_request(f'product_images/{product_id}')
    if not data:
        return []
    images = []
    if data.get('main'):
        images.append({'src': data['main']})
    for img in (data.get('others') or []):
        if img:
            images.append({'src': img})
    return images


def get_location_id():
    r = shopify_request('GET', 'locations.json')
    if not r:
        return None
    locs = r.json().get('locations', [])
    return locs[0]['id'] if locs else None


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


def sync():
    print('=== Sincronizando JimSports -> Shopify ===')

    location_id = get_location_id()
    if not location_id:
        print('ERROR: no se encontró location en Shopify')
        return
    print(f'Location id: {location_id}')

    print('Listando productos JimSports ya en Shopify...')
    existing = fetch_existing_by_sku()
    print(f'  {len(existing)} ya existen')

    print('Obteniendo lista de Jim Sports...')
    jim_ids = jim_request('products')
    if not jim_ids:
        print('ERROR: no se pudieron obtener productos de Jim Sports')
        return
    if SYNC_LIMIT:
        jim_ids = jim_ids[:SYNC_LIMIT]
    total = len(jim_ids)
    print(f'  {total} productos a procesar')

    print('Obteniendo precios y stock...')
    prices_raw = jim_request('prices') or []
    prices = {str(p['product_id']): str(p.get('price', '0.00')) for p in prices_raw}
    stock_raw = jim_request('stock') or []
    stocks = {str(s['product_id']): int(s.get('stock', 0)) for s in stock_raw}

    created = updated = skipped = 0

    for i, jim_id in enumerate(jim_ids, 1):
        jim_id = str(jim_id)
        product = jim_request(f'product/{jim_id}')
        if not product:
            skipped += 1
            continue

        ean = (product.get('ean13') or '').strip()
        if not ean:
            skipped += 1
            continue

        name_obj = product.get('name') or {}
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'
        desc_obj = product.get('description') or {}
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''

        price = prices.get(jim_id, '0.00')
        stock = stocks.get(jim_id, 0)

        brand = (product.get('brand') or {}).get('name', '') if isinstance(product.get('brand'), dict) else ''
        cat_id = str(product.get('category_id', ''))
        brand_tag = brand.lower().replace(' ', '-') if brand else 'sin-marca'

        if ean in existing:
            info = existing[ean]
            shopify_request('PUT', f'variants/{info["variant_id"]}.json', data={
                'variant': {'id': info['variant_id'], 'price': price}
            })
            if info.get('inventory_item_id'):
                set_inventory(info['inventory_item_id'], location_id, stock)
            updated += 1
        else:
            images = jim_get_images(jim_id)
            time.sleep(0.2)
            r = shopify_request('POST', 'products.json', data={
                'product': {
                    'title': name,
                    'body_html': desc,
                    'handle': f'jimsports-{ean}',
                    'vendor': brand,
                    'tags': f'jimsports,marca-{brand_tag},cat-{cat_id}',
                    'images': images,
                    'variants': [{
                        'sku': ean,
                        'price': price,
                        'inventory_management': 'shopify'
                    }]
                }
            })
            if r:
                p = r.json().get('product', {})
                v = (p.get('variants') or [{}])[0]
                if v.get('inventory_item_id'):
                    set_inventory(v['inventory_item_id'], location_id, stock)
                created += 1
            else:
                skipped += 1

        if i % 25 == 0:
            print(f'[{i}/{total}] {created} creados, {updated} actualizados, {skipped} errores')

        time.sleep(0.4)

    print(f'FINAL: {created} creados, {updated} actualizados, {skipped} errores')


if __name__ == '__main__':
    sync()
    print('Sync completado')
