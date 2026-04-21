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

# ─── SHOPIFY ───────────────────────────────────────────────────────────────────

def shopify_get(endpoint):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    r = requests.get(url, headers=HEADERS_SHOPIFY)
    r.raise_for_status()
    return r.json()

def shopify_post(endpoint, data):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    r = requests.post(url, headers=HEADERS_SHOPIFY, json=data)
    r.raise_for_status()
    return r.json()

def shopify_put(endpoint, data):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    r = requests.put(url, headers=HEADERS_SHOPIFY, json=data)
    r.raise_for_status()
    return r.json()

def shopify_delete(endpoint):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    r = requests.delete(url, headers=HEADERS_SHOPIFY)
    return r.status_code

# ─── JIM SPORTS ────────────────────────────────────────────────────────────────

def jim_get(endpoint):
    url = f'https://api.jimsports.com/v1/{endpoint}'
    r = requests.get(url, headers=HEADERS_JIM)
    r.raise_for_status()
    return r.json()

def jim_get_images(product_id):
    try:
        data = jim_get(f'product_images/{product_id}')
        images = []
        if data.get('main'):
            images.append({'src': data['main']})
        for img in data.get('others', []):
            if img:
                images.append({'src': img})
        return images
    except:
        return []

# ─── PASO 1: BORRAR PRODUCTOS EXISTENTES ──────────────────────────────────────

def delete_all_jimsports_products():
    print('\n=== Borrando productos Jim Sports existentes ===')
    deleted = 0
    while True:
        data = shopify_get('products.json?limit=250&fields=id,tags')
        products = [p for p in data.get('products', []) if 'jimsports' in p.get('tags', '')]
        if not products:
            break
        for p in products:
            shopify_delete(f'products/{p["id"]}.json')
            deleted += 1
            time.sleep(0.3)
        print(f'  {deleted} productos borrados...')
    print(f'  ✓ Total borrados: {deleted}')

# ─── PASO 2: SINCRONIZAR COLECCIONES ──────────────────────────────────────────

def sync_collections():
    print('\n=== Sincronizando colecciones ===')
    categories = jim_get('categories')
    collection_map = {}

    for cat in categories:
        cat_id = str(cat.get('id', ''))
        name_obj = cat.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Categoria {cat_id}'
        handle = f'jimsports-cat-{cat_id}'

        # Buscar si ya existe
        data = shopify_get(f'custom_collections.json?handle={handle}')
        cols = data.get('custom_collections', [])
        if cols:
            shopify_id = cols[0]['id']
        else:
            result = shopify_post('custom_collections.json', {
                'custom_collection': {
                    'title': name,
                    'handle': handle,
                    'published': True
                }
            })
            shopify_id = result['custom_collection']['id']
            print(f'  ✓ Creada: {name}')

        collection_map[cat_id] = shopify_id
        time.sleep(0.3)

    print(f'  ✓ {len(collection_map)} colecciones listas')
    return collection_map

# ─── PASO 3: SINCRONIZAR PRODUCTOS ────────────────────────────────────────────

def sync_products(collection_map):
    print('\n=== Sincronizando productos ===')

    print('Obteniendo IDs de Jim Sports...')
    jim_ids = jim_get('products')
    print(f'  {len(jim_ids)} productos en Jim Sports')

    print('Obteniendo precios...')
    prices_raw = jim_get('prices')
    prices = {str(p['product_id']): str(p.get('price', '0.00')) for p in prices_raw}

    print('Obteniendo stock...')
    stock_raw = jim_get('stock')
    stock_data = {str(s['product_id']): int(s.get('stock', 0)) for s in stock_raw}

    created = skipped = 0

    for i, jim_id in enumerate(jim_ids):
        jim_id = str(jim_id)

        try:
            product = jim_get(f'product/{jim_id}')
        except Exception as e:
            print(f'  ✗ Error producto {jim_id}: {e}')
            skipped += 1
            time.sleep(1)
            continue

        ean = product.get('ean13', '')
        handle = f'jimsports-{ean}' if ean else f'jimsports-id-{jim_id}'

        name_obj = product.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'

        desc_obj = product.get('description', {})
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''

        price = prices.get(jim_id, '0.00')
        stock = stock_data.get(jim_id, 0)
        brand = product.get('brand', {}).get('name', '') if product.get('brand') else ''
        cat_id = str(product.get('category_id', ''))

        # Obtener imágenes
        images = jim_get_images(jim_id)

        # Crear producto
        try:
            result = shopify_post('products.json', {
                'product': {
                    'title': name,
                    'body_html': desc,
                    'handle': handle,
                    'vendor': brand,
                    'tags': f'jimsports,jimsports-cat-{cat_id}',
                    'images': images,
                    'variants': [{
                        'sku': ean,
                        'price': price,
                        'inventory_quantity': stock,
                        'inventory_management': 'shopify'
                    }]
                }
            })
            new_id = result['product']['id']

            # Añadir a colección
            col_id = collection_map.get(cat_id)
            if col_id:
                try:
                    shopify_post('collects.json', {
                        'collect': {
                            'collection_id': col_id,
                            'product_id': new_id
                        }
                    })
                except:
                    pass

            created += 1

        except Exception as e:
            print(f'  ✗ Error creando {name}: {e}')
            skipped += 1

        if (i + 1) % 50 == 0:
            print(f'  Progreso: {i+1}/{len(jim_ids)} — {created} creados, {skipped} errores')

        time.sleep(0.4)

    print(f'  ✓ Completado: {created} creados, {skipped} errores')

# ─── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('==============================')
    print('  JimSports → Shopify Sync')
    print('==============================')
    delete_all_jimsports_products()
    collection_map = sync_collections()
    sync_products(collection_map)
    print('\n✓ Sync completado')
