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

# ─── SHOPIFY HELPERS ───────────────────────────────────────────────────────────

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

# ─── JIM SPORTS HELPERS ────────────────────────────────────────────────────────

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

# ─── COLECCIONES ───────────────────────────────────────────────────────────────

def get_or_create_shopify_collection(title, handle):
    """Busca una colección por handle, si no existe la crea."""
    data = shopify_get(f'custom_collections.json?handle={handle}')
    cols = data.get('custom_collections', [])
    if cols:
        return cols[0]['id']
    # Crear
    result = shopify_post('custom_collections.json', {
        'custom_collection': {
            'title': title,
            'handle': handle,
            'published': True
        }
    })
    print(f'  ✓ Colección creada: {title}')
    return result['custom_collection']['id']

def add_product_to_collection(collection_id, product_id):
    try:
        shopify_post('collects.json', {
            'collect': {
                'collection_id': collection_id,
                'product_id': product_id
            }
        })
    except:
        pass  # Ya existe el enlace

def sync_collections():
    """Sincroniza categorías de Jim Sports como colecciones en Shopify."""
    print('\n=== Sincronizando colecciones ===')
    categories = jim_get('categories')
    collection_map = {}  # jim_category_id -> shopify_collection_id

    for cat in categories:
        cat_id = str(cat.get('id', ''))
        name_obj = cat.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Categoria {cat_id}'
        handle = f'jimsports-{cat_id}'
        shopify_id = get_or_create_shopify_collection(name, handle)
        collection_map[cat_id] = shopify_id
        time.sleep(0.3)

    print(f'  {len(collection_map)} colecciones sincronizadas')
    return collection_map

# ─── PRODUCTOS ─────────────────────────────────────────────────────────────────

def get_all_shopify_products():
    """Obtiene todos los productos de Shopify con tag jimsports."""
    products = {}
    url = 'products.json?limit=250&fields=id,handle,variants,tags'
    while url:
        data = shopify_get(url)
        for p in data.get('products', []):
            if 'jimsports' in p.get('tags', ''):
                products[p['handle']] = p
        # Paginación
        url = None  # Simplificado - añadir paginación si >250 productos
    return products

def get_prices():
    prices = jim_get('prices')
    return {str(p['product_id']): str(p.get('price', '0.00')) for p in prices}

def get_stock():
    stock = jim_get('stock')
    return {str(s['product_id']): int(s.get('stock', 0)) for s in stock}

def sync_products(collection_map):
    """Sincroniza productos de Jim Sports en Shopify."""
    print('\n=== Sincronizando productos ===')

    # Obtener datos Jim Sports
    print('Obteniendo lista de productos...')
    jim_product_ids = jim_get('products')  # Devuelve lista de IDs
    print(f'  {len(jim_product_ids)} productos en Jim Sports')

    print('Obteniendo precios...')
    prices = get_prices()

    print('Obteniendo stock...')
    stock_data = get_stock()

    # Productos actuales en Shopify
    print('Obteniendo productos actuales en Shopify...')
    existing = get_all_shopify_products()
    print(f'  {len(existing)} productos Jim Sports ya en Shopify')

    created = updated = skipped = 0

    for i, jim_id in enumerate(jim_product_ids):
        jim_id = str(jim_id)

        try:
            product = jim_get(f'product/{jim_id}')
        except Exception as e:
            print(f'  Error obteniendo producto {jim_id}: {e}')
            skipped += 1
            continue

        ean = product.get('ean13', '')
        handle = f'jimsports-{ean}' if ean else f'jimsports-{jim_id}'

        name_obj = product.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'

        desc_obj = product.get('description', {})
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''

        price = prices.get(jim_id, '0.00')
        stock = stock_data.get(jim_id, 0)
        brand = product.get('brand', {}).get('name', '')

        # Colección del producto
        cat_id = str(product.get('category_id', ''))
        shopify_collection_id = collection_map.get(cat_id)

        if handle in existing:
            # Actualizar precio y stock
            shopify_id = existing[handle]['id']
            variant_id = existing[handle]['variants'][0]['id']
            shopify_put(f'products/{shopify_id}.json', {
                'product': {
                    'id': shopify_id,
                    'variants': [{
                        'id': variant_id,
                        'price': price,
                        'inventory_quantity': stock
                    }]
                }
            })
            updated += 1
        else:
            # Crear producto con imágenes
            images = jim_get_images(jim_id)
            result = shopify_post('products.json', {
                'product': {
                    'title': name,
                    'body_html': desc,
                    'handle': handle,
                    'vendor': brand,
                    'tags': 'jimsports',
                    'images': images,
                    'variants': [{
                        'sku': ean,
                        'price': price,
                        'inventory_quantity': stock,
                        'inventory_management': 'shopify'
                    }]
                }
            })
            new_shopify_id = result['product']['id']

            # Añadir a colección
            if shopify_collection_id:
                add_product_to_collection(shopify_collection_id, new_shopify_id)

            created += 1

        if (created + updated) % 20 == 0 and (created + updated) > 0:
            print(f'  Progreso: {created} creados, {updated} actualizados, {skipped} errores...')

        time.sleep(0.3)

    print(f'  ✓ Completado: {created} creados, {updated} actualizados, {skipped} errores')

# ─── MAIN ──────────────────────────────────────────────────────────────────────

def sync():
    print('==============================')
    print('  JimSports → Shopify Sync')
    print('==============================')
    collection_map = sync_collections()
    sync_products(collection_map)
    print('\n✓ Sync completado')

if __name__ == '__main__':
    sync()
