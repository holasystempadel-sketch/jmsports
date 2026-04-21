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

# ─── HELPERS CON RETRY ────────────────────────────────────────────────────────

def shopify_request(method, endpoint, data=None, retries=5):
    url = f'https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}'
    for attempt in range(retries):
        try:
            if method == 'GET':
                r = requests.get(url, headers=HEADERS_SHOPIFY)
            elif method == 'POST':
                r = requests.post(url, headers=HEADERS_SHOPIFY, json=data)
            elif method == 'PUT':
                r = requests.put(url, headers=HEADERS_SHOPIFY, json=data)
            elif method == 'DELETE':
                r = requests.delete(url, headers=HEADERS_SHOPIFY)
                return r.status_code

            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 10))
                print(f'  ⏳ Rate limit Shopify, esperando {wait}s...')
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'  ⚠️  Error intento {attempt+1}: {e}, reintentando...')
                time.sleep(5)
            else:
                raise
    return None

def jim_request(endpoint, retries=5):
    url = f'https://api.jimsports.com/v1/{endpoint}'
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS_JIM)
            if r.status_code == 429:
                print(f'  ⏳ Rate limit Jim Sports, esperando 10s...')
                time.sleep(10)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f'  ⚠️  Error intento {attempt+1}: {e}, reintentando...')
                time.sleep(5)
            else:
                print(f'  ✗ Fallo definitivo en {endpoint}: {e}')
                return None
    return None

# ─── PAGINACIÓN SHOPIFY ───────────────────────────────────────────────────────

def shopify_get_all_products():
    """Obtiene todos los productos con paginación correcta por since_id."""
    products = []
    since_id = 0
    while True:
        data = shopify_request('GET', f'products.json?limit=250&since_id={since_id}&fields=id,handle,tags,variants')
        if not data:
            break
        batch = data.get('products', [])
        if not batch:
            break
        products.extend(batch)
        print(f'  {len(products)} productos obtenidos de Shopify...')
        if len(batch) < 250:
            break
        since_id = batch[-1]['id']
        time.sleep(0.5)
    return products

# ─── IMÁGENES ─────────────────────────────────────────────────────────────────

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

# ─── COLECCIONES ──────────────────────────────────────────────────────────────

def sync_collections():
    print('\n=== Sincronizando colecciones ===')
    categories = jim_request('categories')
    if not categories:
        print('  ✗ No se pudieron obtener categorías')
        return {}

    collection_map = {}

    for cat in categories:
        cat_id = str(cat.get('id', ''))
        name_obj = cat.get('name', {})
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Categoria {cat_id}'
        handle = f'jimsports-cat-{cat_id}'

        data = shopify_request('GET', f'custom_collections.json?handle={handle}')
        cols = data.get('custom_collections', []) if data else []

        if cols:
            shopify_id = cols[0]['id']
            print(f'  → Ya existe: {name}')
        else:
            result = shopify_request('POST', 'custom_collections.json', {
                'custom_collection': {
                    'title': name,
                    'handle': handle,
                    'published': True
                }
            })
            if result:
                shopify_id = result['custom_collection']['id']
                print(f'  ✓ Creada: {name}')
            else:
                print(f'  ✗ Error creando: {name}')
                continue

        collection_map[cat_id] = shopify_id
        time.sleep(0.5)

    print(f'  ✓ {len(collection_map)} colecciones listas')
    return collection_map

# ─── PRODUCTOS ────────────────────────────────────────────────────────────────

def sync_products(collection_map):
    print('\n=== Sincronizando productos ===')

    print('Obteniendo IDs de Jim Sports...')
    jim_ids = jim_request('products')
    if not jim_ids:
        print('  ✗ No se pudieron obtener productos')
        return
    print(f'  {l
