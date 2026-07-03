"""
JimSports -> Shopify sync v3 -- replica fidel de l'abast System Padel.

Canvis respecte al v2 (github_upload/sync_v2.py):
  - FILTRE D'ABAST: nomes importa les categories del menu de la web
    (Padel, Entrenamiento, R&P, Equipamiento). La resta d'esports de Jim
    (Casual, Fitness, Natacion, Futbol...) NO s'importen.
  - RECONCILIACIO COMPLETA de productes existents: compara opcions i
    valors (Color/Talla) de cada variant amb el que hi ha a Shopify i
    reconstrueix el set de variants si difereixen -> arregla variants
    trencades i n'elimina les fantasma dins de productes vius.
  - FASE PRUNE: al final de cada run complet, ELIMINA de Shopify qualsevol
    producte amb tag `jimsports` que ja no formi part del cataleg en abast
    de Jim (descatalogats, web:false, fora d'abast). Amb guards de seguretat:
    nomes si el run es complet (SYNC_LIMIT=0) i amb pocs errors.
  - Tag `bajo-demanda` per a productes on_demand (porteries, etc.).
  - Actualitza titol/descripcio a mes de tags/preu/stock (paritat amb Jim).
  - ONLY_NEW eliminat: cada run reconcilia tot l'abast.
  - SKU = referencia Jim (ex. 77542.019.2) i barcode = EAN13. L'aparellament
    de variants es fa per BARCODE (EAN, clau estable); el primer run despres
    d'aquest canvi migra els SKUs antics (que eren l'EAN) conservant els ids.

Variables d'entorn:
  JIMSPORTS_API_KEY  (secret) -- clau ClientAuth
  SHOPIFY_TOKEN      (secret) -- Admin API Access Token
  SHOPIFY_STORE      (defecte 'xqksc3-ua.myshopify.com')
  PRICE_MULTIPLIER   (defecte 2.0)
  SYNC_LIMIT         (defecte 0 = tots; si >0 la fase prune NO s'executa)
  DRY_RUN            (defecte false -- imprimeix sense tocar res)
  PRUNE              (defecte true -- posa false per desactivar les eliminacions)
  DEBUG_REF          (opcional, ex. 'A005504')
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
SYNC_LIMIT        = int(os.environ.get('SYNC_LIMIT') or '0')
PRICE_MULTIPLIER  = float(os.environ.get('PRICE_MULTIPLIER') or '2.0')
DEBUG_REF         = (os.environ.get('DEBUG_REF') or '').strip()
DRY_RUN           = (os.environ.get('DRY_RUN') or 'false').strip().lower() == 'true'
PRUNE             = (os.environ.get('PRUNE') or 'true').strip().lower() == 'true'
# Pausa entre peticions a Jim: sense aquesta pausa l'API retorna 429 (rate limit)
# i cada producte acaba trigant ~25s en reintents. Amb 0.35s va fluid (com el v2).
JIM_DELAY         = float(os.environ.get('JIM_DELAY') or '0.35')

# --- ABAST: categories Jim que corresponen al menu de systempadel.com ---------
# Padel . Entrenamiento (+Psicomotricidad) . R&P . Equipamiento
# (Novedades i Outlet son flags transversals dins d'aquest abast.)
SCOPE_CATS = {
    # Padel
    1192, 1193, 1194, 1195, 1196, 1197, 1262,
    # Entrenamiento + Psicomotricidad
    1158, 1159, 1160, 1205, 1206, 1207, 1272, 1295, 1296, 1298, 1299,
    # R&P (badminton, pickleball, tenis, tenis de mesa, frontenis)
    1124, 1125, 1202, 1203, 1218, 1219, 1221, 1222, 1271,
    # Equipamiento
    1161, 1257, 1258, 1259, 1260, 1261, 1263, 1264, 1265, 1266, 1267, 1268, 1269, 1270,
}
_scope_env = (os.environ.get('SCOPE_CATS') or '').strip()
if _scope_env:
    SCOPE_CATS = {int(x) for x in _scope_env.split(',') if x.strip().isdigit()}

# attribute_id de Jim -> nom d'opcio Shopify (decodificacio nativa de /v1/attribute_values)
ATTR_OPT = {34:'Color', 21:'Talla calzado', 29:'Talla textil', 87:'Dise\u00f1o',
            22:'N\u00famero', 3:'Tama\u00f1o', 13:'Talla bal\u00f3n', 6:'Talla calcet\u00edn',
            11:'Peso', 41:'Pulgadas', 92:'Tejido', 79:'Talla y lado',
            32:'Medidas', 24:'Medida', 76:'Densidad', 77:'Medida y densidad',
            72:'Fen\u00f3lico', 33:'Modelo'}
ATTR_SKIP  = {16, 5, 8, 46, 52}  # Categoria/Cajas/Cantidades/Palas/Overgrips (no son opcions)
ATTR_ORDER = ['Color','Talla calzado','Talla textil','Talla bal\u00f3n','Talla calcet\u00edn',
              'N\u00famero','Tama\u00f1o','Talla y lado','Pulgadas','Medida','Medidas',
              'Densidad','Medida y densidad','Peso','Tejido','Fen\u00f3lico','Dise\u00f1o','Modelo']

SHOPIFY_BASE = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}'
SHOPIFY_GQL  = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'
ONLINE_STORE_PUB_ID = 'gid://shopify/Publication/325355798910'

HEADERS_JIM = {
    'ClientAuth': JIMSPORTS_API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'JimSports-Shopify-Sync/3.0',
}
HEADERS_SHOPIFY = {
    'X-Shopify-Access-Token': SHOPIFY_TOKEN,
    'Content-Type': 'application/json',
}

# --- HTTP HELPERS -------------------------------------------------------------

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
                print(f'  Rate limit Jim ({endpoint}), esperant 10s')
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


# --- PUBLICACIO ---------------------------------------------------------------

def publish_to_online_store(product_gid):
    """Publica un producte al canal Online Store via GraphQL."""
    mutation = '''mutation($id: ID!) {
      publishablePublish(id: $id, input: [{publicationId: "%s"}]) {
        userErrors { message }
      }
    }''' % ONLINE_STORE_PUB_ID
    payload = json.dumps({'query': mutation, 'variables': {'id': product_gid}})
    for attempt in range(3):
        try:
            r = requests.post(SHOPIFY_GQL, headers=HEADERS_SHOPIFY, data=payload, timeout=30)
            if r.status_code == 200:
                ue = r.json().get('data', {}).get('publishablePublish', {}).get('userErrors', [])
                if ue:
                    print(f'    publish error: {ue[0]["message"]}')
                return
            if r.status_code == 429:
                time.sleep(2)
                continue
        except Exception as e:
            print(f'    publish exception: {e}')
            time.sleep(1)


def pvp(raw):
    try:
        return f'{round(float(raw or 0) * PRICE_MULTIPLIER, 2):.2f}'
    except (TypeError, ValueError):
        return '0.00'


# --- HANDLES ------------------------------------------------------------------

def slugify(text):
    import unicodedata
    text = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')[:200]


# --- MAPPINGS GLOBALS ---------------------------------------------------------

def fetch_brand_map():
    data = jim_request('brands') or []
    return {b['id']: (b.get('name') or '').strip() for b in data}


def fetch_attribute_value_label():
    """Retorna {attribute_value_id: (attribute_id, 'NOM')} en castella."""
    data = jim_request('attribute_values') or []
    out = {}
    for av in data:
        name_obj = av.get('name') or {}
        name = name_obj.get('es-ES') or name_obj.get('en-US') or str(av['id'])
        out[av['id']] = (av.get('attribute_id'), name)
    return out


def fetch_category_map():
    """Retorna {category_id: 'Nom de la categoria'} en castella."""
    data = jim_request('categories') or []
    out = {}
    for c in data:
        name_obj = c.get('name') or {}
        name = (name_obj.get('es-ES') or name_obj.get('en-US') or '').strip()
        if name:
            out[c['id']] = name
    return out


# --- PRODUCTES SHOPIFY EXISTENTS ----------------------------------------------

def fetch_existing():
    """Retorna (existing{ean:...}, handles, by_ref, by_handle, shop_products{pid:...}).
    shop_products guarda opcions i variants senceres per poder reconciliar
    sense tornar a demanar cada producte, i el set de productes jimsports
    per a la fase prune."""
    existing = {}
    handles = set()
    by_ref = {}
    by_handle = {}
    shop_products = {}
    url = (f'{SHOPIFY_BASE}/products.json?limit=250'
           f'&fields=id,handle,title,tags,options,variants')
    while url:
        r = shopify_request('GET', url)
        if not r:
            break
        for p in r.json().get('products', []):
            if p.get('handle'):
                handles.add(p['handle'])
                by_handle[p['handle']] = p['id']
            raw_tags = p.get('tags') or ''
            if 'jimsports' not in raw_tags.lower():
                continue
            shop_products[p['id']] = {
                'handle': p.get('handle'),
                'title': p.get('title') or '',
                'tags': {t.strip() for t in raw_tags.split(',') if t.strip()},
                'options': [o.get('name') for o in (p.get('options') or [])],
                'variants': [{
                    'id': v['id'],
                    'sku': (v.get('sku') or '').strip(),
                    'barcode': (v.get('barcode') or '').strip(),
                    'price': v.get('price'),
                    'option1': v.get('option1'), 'option2': v.get('option2'),
                    'option3': v.get('option3'),
                    'inventory_item_id': v.get('inventory_item_id'),
                    'inventory_quantity': v.get('inventory_quantity'),
                } for v in (p.get('variants') or [])],
            }
            for t in raw_tags.split(','):
                t = t.strip().lower()
                if t.startswith('ref-') and t not in by_ref:
                    by_ref[t] = p['id']
            for v in p.get('variants', []):
                sku = (v.get('sku') or '').strip()
                barcode = (v.get('barcode') or '').strip()
                info = {
                    'product_id': p['id'],
                    'variant_id': v['id'],
                    'inventory_item_id': v.get('inventory_item_id'),
                }
                # indexem per EAN (barcode) com a clau principal i per SKU
                # com a secundaria (els SKUs antics eren l'EAN)
                if barcode and barcode not in existing:
                    existing[barcode] = info
                if sku and sku not in existing:
                    existing[sku] = info
        url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                url = part.split(';')[0].strip().strip('<>')
                break
    return existing, handles, by_ref, by_handle, shop_products


# --- INVENTARI ----------------------------------------------------------------

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


# --- VARIANTS -----------------------------------------------------------------

def _variant_attrs(v, val_attr):
    """Decodifica l'attribute_value d'una variant -> {opcio: valor}."""
    d = {}
    for avid in (v.get('attribute_value') or []):
        aid, name = val_attr.get(avid, (None, None))
        if name is None or aid in ATTR_SKIP:
            continue
        opt = ATTR_OPT.get(aid)
        if opt:
            d[opt] = name
    return d


def build_variants(product, val_attr):
    """Construeix variants amb opcions netes decodificant `attribute_value`
    de cada variant (mai la referencia). Fallback a "Variante"."""
    from collections import defaultdict
    raw = [v for v in (product.get('variants') or []) if not v.get('discontinued')]

    # Cas A: 0 o 1 variant -> producte simple
    if len(raw) <= 1:
        v = raw[0] if raw else product
        ean = v.get('ean13') or product.get('ean13')
        sku = v.get('reference') or product.get('reference') or ean
        if not sku:
            return None, None, 'no-ean'
        return [{
            'sku': sku, 'barcode': ean or '',
            'price': pvp(v.get('price') or product.get('price')),
            'inventory_management': 'shopify',
            '_stock': int(v.get('stock') or product.get('stock') or 0),
        }], None, None

    # Cas B: multiples variants -> decodificar attribute_value
    rows = []
    for v in raw:
        ean = v.get('ean13')
        sku = v.get('reference') or ean
        if not sku:
            continue
        rows.append((v, sku, ean, _variant_attrs(v, val_attr)))
    if not rows:
        return None, None, 'no-ean'

    decoded = [r for r in rows if r[3]]
    chosen = []
    if decoded:
        counts = defaultdict(set)
        for _, _, _, d in decoded:
            for k, val in d.items():
                counts[k].add(val)
        in_all = [k for k in counts if all(k in d for _, _, _, d in decoded)]
        chosen = [k for k in ATTR_ORDER if k in in_all and (len(counts[k]) >= 2 or k == 'Color')]
        chosen += [k for k in in_all if k not in chosen and len(counts[k]) >= 2 and k not in ATTR_ORDER]
        chosen = chosen[:3]
        if chosen and not all(all(k in d for k in chosen) for _, _, _, d in rows):
            chosen = []

    if chosen:
        out = []; seen = set()
        for v, sku, ean, d in rows:
            combo = tuple(d[k] for k in chosen); c2 = combo; idx = 2
            while c2 in seen:
                c2 = combo[:-1] + (f'{combo[-1]} ({idx})',); idx += 1
            seen.add(c2)
            vd = {'sku': sku, 'barcode': ean or '',
                  'price': pvp(v.get('price') or product.get('price')),
                  'inventory_management': 'shopify',
                  '_stock': int(v.get('stock') or 0)}
            for i, val in enumerate(c2, 1):
                vd[f'option{i}'] = val
            out.append(vd)
        return out, chosen, None

    out = []; seen = set()
    for v, sku, ean, d in rows:
        label = ' / '.join(d[k] for k in ATTR_ORDER if k in d) or sku
        original, idx = label, 2
        while label in seen:
            label = f'{original} ({idx})'; idx += 1
        seen.add(label)
        out.append({
            'sku': sku, 'barcode': ean or '', 'option1': label,
            'price': pvp(v.get('price') or product.get('price')),
            'inventory_management': 'shopify',
            '_stock': int(v.get('stock') or 0),
        })
    return out, ['Variante'], None


# --- RECONCILIACIO ------------------------------------------------------------

def _vkey(sku, barcode):
    """Clau estable d'una variant: EAN (barcode) si en te, sino el SKU."""
    return (barcode or sku or '').strip()


def needs_rebuild(shop_p, variants, option_names):
    """True si el set de variants de Shopify no coincideix amb el desitjat
    (opcions, SKUs, EANs o valors Color/Talla) -> cal reconstruir-lo sencer.
    La clau de comparacio es l'EAN; el SKU forma part de la signatura, aixi
    un canvi de SKU (migracio EAN -> referencia Jim) tambe dispara el rebuild."""
    desired_opts = option_names or []
    shop_opts = [o for o in (shop_p.get('options') or []) if o and o != 'Title']
    if [o for o in desired_opts] != shop_opts:
        return True
    desired = {}
    for v in variants:
        opts = tuple(v.get(f'option{i}') for i in (1, 2, 3)
                     if v.get(f'option{i}') is not None)
        desired[_vkey(v['sku'], v.get('barcode'))] = (v['sku'],) + opts
    actual = {}
    for v in shop_p['variants']:
        opts = tuple(x for x in (v['option1'], v['option2'], v['option3'])
                     if x is not None and x != 'Default Title')
        actual[_vkey(v['sku'], v.get('barcode'))] = (v['sku'],) + opts
    return desired != actual


def rebuild_product_variants(pid, shop_p, variants, option_names, location_id):
    """PUT del producte amb el set de variants complet: Shopify actualitza les
    que porten id (aparellades per EAN), crea les noves i ELIMINA les que no
    hi son (fantasmes). Aparellant per EAN es conserven els ids de variant
    encara que el SKU canvii (migracio EAN -> referencia Jim)."""
    by_key = {_vkey(v['sku'], v.get('barcode')): v for v in shop_p['variants']}
    payload_variants = []
    for vd in variants:
        pv = {k: v for k, v in vd.items() if not k.startswith('_')}
        ex = by_key.get(_vkey(vd['sku'], vd.get('barcode')))
        if ex:
            pv['id'] = ex['id']
        payload_variants.append(pv)
    data = {'product': {'id': pid, 'variants': payload_variants}}
    if option_names:
        data['product']['options'] = [{'name': n} for n in option_names]
    else:
        # producte que queda amb 1 sola variant -> opcio per defecte de Shopify
        data['product']['options'] = [{'name': 'Title'}]
        for pv in payload_variants:
            pv['option1'] = 'Default Title'
    r = shopify_request('PUT', f'products/{pid}.json', data=data)
    if not (r and r.json().get('product')):
        return False
    # stock de totes les variants resultants (per EAN)
    stock_by_key = {_vkey(v['sku'], v.get('barcode')): v['_stock'] for v in variants}
    for sv in r.json()['product'].get('variants', []):
        key = _vkey(sv.get('sku'), sv.get('barcode'))
        if key in stock_by_key and sv.get('inventory_item_id'):
            set_inventory(sv['inventory_item_id'], location_id, stock_by_key[key])
    return True


# --- COL.LECCIONS PER MARCA ---------------------------------------------------

def ensure_brand_collection(brand_name, cache):
    if not brand_name:
        return None
    if brand_name in cache:
        return cache[brand_name]
    slug = re.sub(r'[^a-z0-9]+', '-', brand_name.lower()).strip('-')
    handle = f'marca-{slug}'
    r = shopify_request('GET', f'smart_collections.json?handle={handle}')
    if r:
        cols = r.json().get('smart_collections', [])
        if cols:
            cache[brand_name] = cols[0]['id']
            return cache[brand_name]
    r = shopify_request('POST', 'smart_collections.json', data={
        'smart_collection': {
            'title': brand_name,
            'handle': handle,
            'published': True,
            'template_suffix': 'bulk',
            'rules': [{'column': 'tag', 'relation': 'equals', 'condition': handle}],
            'disjunctive': False,
        },
    })
    if r:
        cid = r.json().get('smart_collection', {}).get('id')
        cache[brand_name] = cid
        return cid
    return None


# --- TAGS ---------------------------------------------------------------------

def build_tags(product, brand):
    brand_slug = re.sub(r'[^a-z0-9]+', '-', brand.lower()).strip('-') if brand else 'sin-marca'
    cats = [f'cat-{c}' for c in (product.get('category_ids') or [])]
    extras = []
    if product.get('new'):        extras.append('novedad')
    if product.get('outlet'):     extras.append('outlet')
    if product.get('on_demand'):  extras.append('bajo-demanda')
    ref = product.get('reference', '')
    if ref:
        extras.append(f'ref-{slugify(ref)}')
    return ','.join(['jimsports', f'marca-{brand_slug}'] + cats + extras)


# --- CICLE PRINCIPAL ----------------------------------------------------------

def sync():
    print('=== JimSports -> Shopify (v3) ===')
    print(f'Store: {SHOPIFY_STORE}  .  x{PRICE_MULTIPLIER}  .  Limit: {SYNC_LIMIT or "TOTS"}'
          f'  .  DRY_RUN: {DRY_RUN}  .  PRUNE: {PRUNE}  .  Abast: {len(SCOPE_CATS)} categories')

    location_id = get_location_id()
    if not location_id:
        print('ERROR: no es troba location a Shopify')
        return
    print(f'Location id: {location_id}')

    brands_map = fetch_brand_map()
    print(f'{len(brands_map)} marques carregades')

    attr_value_label = fetch_attribute_value_label()
    print(f'{len(attr_value_label)} valors d\'atribut carregats')

    cats_map = fetch_category_map()
    print(f'{len(cats_map)} categories carregades')

    existing, existing_handles, by_ref, by_handle, shop_products = fetch_existing()
    print(f'{len(existing)} variants | {len(by_ref)} refs | {len(shop_products)} productes jimsports a Shopify')

    jim_ids = jim_request('products')
    if jim_ids is None:
        print('ERROR: /v1/products no respon -- abortem (cap eliminacio)')
        return
    if SYNC_LIMIT:
        jim_ids = jim_ids[:SYNC_LIMIT]
    total = len(jim_ids)
    print(f'{total} productes Jim a processar\n')

    brand_collection_cache = {}
    touched_ids = set()   # productes Shopify confirmats al cataleg en abast
    created = updated = rebuilt = unchanged = out_scope = skipped = errors = 0

    for i, jim_id in enumerate(jim_ids, 1):
        jim_id = str(jim_id)
        if i % 200 == 0:
            print(f'  ... progres {i}/{total} (creats {created} / reconstr {rebuilt} / act {updated} / fora {out_scope})')
        time.sleep(JIM_DELAY)  # pacing anti-429 (veure JIM_DELAY)
        product = jim_request(f'product/{jim_id}')
        if not product:
            errors += 1
            continue

        ref = product.get('reference', '')
        if DEBUG_REF and ref == DEBUG_REF:
            print(f'\n=== DEBUG_REF {ref} JSON ===')
            print(json.dumps(product, indent=2, ensure_ascii=False)[:5000])
            print('=== /DEBUG ===\n')

        # Regles de publicacio (Jim Sports): descatalogat o ocult a la seva web -> fora
        if product.get('discontinued') or not product.get('web'):
            skipped += 1
            continue

        # FILTRE D'ABAST: alguna categoria del producte dins del menu System Padel
        cat_ids = set(product.get('category_ids') or [])
        if not (cat_ids & SCOPE_CATS):
            out_scope += 1
            continue

        name_obj = product.get('name') or {}
        name = name_obj.get('es-ES') or name_obj.get('en-US') or f'Producto {jim_id}'
        desc_obj = product.get('description') or {}
        desc = desc_obj.get('es-ES') or desc_obj.get('en-US') or ''
        brand = brands_map.get(product.get('brand_id'), '')
        tags = build_tags(product, brand)
        images = [{'src': u} for u in (product.get('images') or []) if u]

        variants, option_names, err = build_variants(product, attr_value_label)
        if err == 'no-ean':
            skipped += 1
            print(f'  [{i}/{total}] {ref} SENSE EAN, skip')
            continue

        product_type = ''
        for cid in (product.get('category_ids') or []):
            if cats_map.get(cid):
                product_type = cats_map[cid]
                break

        # Sense preu = "consultar precio" (decisio Pau + regla Jim)
        try:
            if variants and all(float(v.get('price') or 0) == 0 for v in variants):
                tags = tags + ',consultar-precio'
        except (TypeError, ValueError):
            pass

        # Localitzar el producte existent: ref -> EAN -> handle
        ref_tag = f'ref-{slugify(ref)}' if ref else ''
        existing_pid = by_ref.get(ref_tag)
        if not existing_pid:
            for vd in variants:
                ex = existing.get(_vkey(vd['sku'], vd.get('barcode'))) or existing.get(vd['sku'])
                if ex:
                    existing_pid = ex['product_id']; break
        if not existing_pid:
            existing_pid = by_handle.get(slugify(name))

        if existing_pid:
            touched_ids.add(existing_pid)
            shop_p = shop_products.get(existing_pid)
            tag_set = {t.strip() for t in tags.split(',') if t.strip()}
            rebuild = shop_p and needs_rebuild(shop_p, variants, option_names)
            meta_changed = (not shop_p or shop_p['tags'] != tag_set
                            or shop_p['title'] != name)
            price_stock_changes = []
            if shop_p and not rebuild:
                by_key = {_vkey(v['sku'], v.get('barcode')): v for v in shop_p['variants']}
                for vd in variants:
                    ex_v = by_key.get(_vkey(vd['sku'], vd.get('barcode')))
                    if not ex_v:
                        continue
                    if str(ex_v.get('price')) != str(vd['price']):
                        price_stock_changes.append(('price', ex_v, vd))
                    if (ex_v.get('inventory_quantity') or 0) != vd['_stock']:
                        price_stock_changes.append(('stock', ex_v, vd))

            if not rebuild and not meta_changed and not price_stock_changes:
                unchanged += 1
                continue

            if DRY_RUN:
                updated += 1
                accio = 'reconstruiria' if rebuild else f'actualitzaria ({len(price_stock_changes)} canvis)'
                print(f'  [{i}/{total}] {ref} {name[:40]} -> [DRY] {accio}')
                continue

            if meta_changed:
                shopify_request('PUT', f'products/{existing_pid}.json', data={
                    'product': {
                        'id': existing_pid,
                        'title': name,
                        'body_html': desc,
                        'tags': tags,
                        'vendor': brand or 'Jim Sports',
                        'product_type': product_type,
                        'template_suffix': 'bulk',
                    },
                })
            if rebuild:
                ok = rebuild_product_variants(existing_pid, shop_p, variants,
                                              option_names, location_id)
                if ok:
                    rebuilt += 1
                    print(f'  [{i}/{total}] {ref} {name[:40]} -> RECONSTRUIT ({len(variants)} variants)')
                else:
                    errors += 1
            else:
                for kind, ex_v, vd in price_stock_changes:
                    if kind == 'price':
                        shopify_request('PUT', f'variants/{ex_v["id"]}.json', data={
                            'variant': {'id': ex_v['id'], 'price': vd['price']},
                        })
                    elif ex_v.get('inventory_item_id'):
                        set_inventory(ex_v['inventory_item_id'], location_id, vd['_stock'])
                if price_stock_changes or meta_changed:
                    updated += 1
                    print(f'  [{i}/{total}] {ref} {name[:40]} -> actualitzat')
            time.sleep(0.3)
        else:
            if DRY_RUN:
                created += 1
                print(f'  [{i}/{total}] {ref} {name[:40]} -> [DRY] crearia ({len(variants)} variants)')
                continue
            handle = slugify(name) or f'producto-{slugify(ref)}'
            base_handle, n = handle, 2
            while handle in existing_handles:
                handle = f'{base_handle}-{n}'
                n += 1
            existing_handles.add(handle)
            product_payload = {
                'title': name, 'body_html': desc,
                'vendor': brand or 'Jim Sports',
                'product_type': product_type,
                'tags': tags, 'images': images,
                'template_suffix': 'bulk', 'handle': handle,
            }
            if len(variants) > 1:
                product_payload['options'] = [{'name': n2} for n2 in (option_names or ['Variante'])]
            product_payload['variants'] = [
                {k: v for k, v in vd.items() if not k.startswith('_')}
                for vd in variants
            ]
            r = shopify_request('POST', 'products.json', data={'product': product_payload})
            if r and r.json().get('product'):
                created_p = r.json()['product']
                touched_ids.add(created_p['id'])
                for sv, payload_v in zip(created_p.get('variants', []), variants):
                    if sv.get('inventory_item_id'):
                        set_inventory(sv['inventory_item_id'], location_id, payload_v['_stock'])
                publish_to_online_store(f'gid://shopify/Product/{created_p["id"]}')
                created += 1
                print(f'  [{i}/{total}] {ref} {name[:40]} -> CREAT ({len(created_p.get("variants", []))} variants)')
                if brand:
                    ensure_brand_collection(brand, brand_collection_cache)
            else:
                errors += 1
            time.sleep(0.4)

    # --- FASE PRUNE: eliminar productes jimsports fora del cataleg en abast --
    pruned = 0
    to_prune = [pid for pid in shop_products if pid not in touched_ids]
    if not PRUNE:
        print(f'\nPRUNE desactivat -- {len(to_prune)} productes quedarien fora (no s\'elimina res)')
    elif SYNC_LIMIT:
        print(f'\nPRUNE saltat (SYNC_LIMIT actiu) -- mai s\'elimina en runs parcials')
    elif errors > max(5, total * 0.01):
        print(f'\nPRUNE saltat per seguretat: {errors} errors durant el run')
    else:
        print(f'\n=== PRUNE: {len(to_prune)} productes a eliminar (fora d\'abast / descatalogats / fantasmes) ===')
        for pid in to_prune:
            info = shop_products[pid]
            if DRY_RUN:
                print(f'  [DRY] eliminaria: {info["title"][:50]} ({info["handle"]})')
                pruned += 1
                continue
            r = shopify_request('DELETE', f'products/{pid}.json')
            if r is not None:
                pruned += 1
                print(f'  eliminat: {info["title"][:50]} ({info["handle"]})')
            else:
                errors += 1
            time.sleep(0.3)

    print('\n=== RESUM ===')
    print(f'Creats:        {created}')
    print(f'Reconstruits:  {rebuilt}')
    print(f'Actualitzats:  {updated}')
    print(f'Sense canvis:  {unchanged}')
    print(f'Fora d\'abast:  {out_scope}')
    print(f'Saltats:       {skipped}')
    print(f'Eliminats:     {pruned}')
    print(f'Errors:        {errors}')


if __name__ == '__main__':
    sync()
    print('Sync v3 acabat')
