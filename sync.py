import os
import requests
import time

JIMSPORTS_API_KEY = os.environ['JIMSPORTS_API_KEY']
SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = 'system-padel.myshopify.com'

HEADERS_JIM = {
    'ClientAuth': JIMSPORTS_API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'JimSports-Shopify-Sync/1.0'
}
HEADERS_SHOPIFY = {
    'X-Shopify-Access-Token': SHOPIFY_TOKEN,
    'Content-Type': 'application/json'
}


def shopify_post(endpoint, data, retries=5):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.post(url, headers=HEADERS_SHOPIFY, json=data)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 10))
                print(f'Rate limit Shopify, esperando {wait}s...')
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'Error intento {attempt+1}: {e}, reintentando...')
                time.sleep(5)
            else:
                print(f'Fallo creando en {endpoint}: {e}')
                return None
    return None


def jim_request(endpoint, retries=5):
    url = f'https://api.jimsports.com/v1/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS_JIM)
            if r.status_code == 429:
                print('Rate limit Jim Sports, esperando 10s...')
                time.sleep(10)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'Error intento {attempt+1}: {e}, reintentando...')
                time.sleep(5)
            else:
                print(f'Fallo definitivo en {endpoint}: {e}')
                return None
    return None


def jim_get_images(product_id):
    data = jim_request(f'product_images/{product_id}')
    if not data:
        return []
    images = []
    if data.get('main'):
        images.append({'src': data['main']})
    for img in data.get('others', []):
        if img:
            images.append({'src': img})
    return images


def sync_products():
    print('=== Sincronizando productos ===')

    print('Obteniendo IDs de Jim Sports...')
    jim_ids = jim_request('products')
    if not jim_ids:
        print('No se pudieron obtener productos')
        return
    print(f'{len(jim_ids)} productos en Jim Sports')

    print('Obteniendo precios...')
    prices_raw = jim_request('prices')
    prices = {str(p['product_id']): str(p.get('price', '0.00')) for p in prices_raw} if prices_raw else {}

    print('Obteniendo stock...')
    stock_raw = jim_request('stock')
    stock_data = {str(s['product_id']): int(s.get('stock', 0)) for s in stock_raw} if stock_raw else {}

    created = 0
    skipped = 0
    total = len(jim_ids)

    for i, jim_id in enumerate(jim_ids):
        jim_id = str(jim_id)
        product = jim_request(f'product/{jim_id}')
        if not product:
            skipped += 1
            continue

        ean = product.get('ean13', '')
        handle = f'jimsports-{ean}' if ean else f'jimsports-id-{jim_id}'

        name_obj = product.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'

        desc_obj = product.get('description', {})
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''

        price = prices.get(jim_id, '0.00')
        stock = stock_data.get(jim_id, 0)

        brand = ''
        if product.get('brand') and isinstance(product['brand'], dict):
            brand = product['brand'].get('name', '')

        cat_id = str(product.get('category_id', ''))
        brand_tag = brand.lower().replace(' ', '-') if brand else 'sin-marca'

        images = jim_get_images(jim_id)
        time.sleep(0.3)

        result = shopify_post('products.json', {
            'product': {
                'title': name,
                'body_html': desc,
                'handle': handle,
                'vendor': brand,
                'tags': f'jimsports,marca-{brand_tag},cat-{cat_id}',
                'images': images,
                'variants': [{
                    'sku': ean,
                    'price': price,
                    'inventory_quantity': stock,
                    'inventory_management': 'shopify'
                }]
            }
        })

        if result:
            created += 1
        else:
            skipped += 1

        if (i + 1) % 100 == 0:
            print(f'[{i+1}/{total}] {created} creados, {skipped} errores')

        time.sleep(0.5)

    print(f'FINAL: {created} creados, {skipped} errores')


if __name__ == '__main__':
    print('==============================')
    print('JimSports -> Shopify Sync')
    print('==============================')
    sync_products()
    print('Sync completado')
