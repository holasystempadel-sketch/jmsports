"""
Reconstrueix les opcions de variant dels productes Shopify (Color / Talla calzado /
Talla textil / Diseño / ...) rellegint-les de Jim Sports per EAN.

Soluciona d'una sola passada (per producte) els errors:
  - Codis de color (A18, 066, 5085...) -> noms reals (Negro, Azul...)
  - Talles de calçat ×10 (380, 420...) -> talla real de Jim (38, 42, 38 1/2...)
  - Opció "Talla calzado" separada de "Talla textil" (segons l'atribut de Jim)
  - Productes amb opcions barrejades (Propulsion, etc.)

MÈTODE FIABLE: cada variant de Jim porta un camp `attribute_value` amb la llista
d'attribute_value_ids reals (ex. [446, 861]). NO es parseja la referència (que conté
codis poc fiables). Cada id es tradueix amb /v1/attribute_values sabent de quin
atribut és (34=Color, 21=Talla calzado, 29=Talla textil, 87=Diseño, 11=Peso...).
El join Shopify<->Jim és per EAN (barcode).

SEGURETAT: un producte només es modifica si TOTES les variants tenen EAN trobat a
Jim i un joc d'opcions coherent. Si no, es SALTA i es reporta (revisió manual).

Variables d'entorn:
  JIMSPORTS_API_KEY  (secret)
  SHOPIFY_TOKEN      (secret)
  SHOPIFY_STORE      (defecte 'xqksc3-ua.myshopify.com')
  DRY_RUN            (defecte 'true')  -> 'false' per aplicar
  LIMIT              (defecte 0 = tots els productes multi-variant)
  ONLY_TITLE         (opcional) -> només productes que contenen aquest text al títol
"""
import os, time, json, threading, requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

JIM_KEY   = os.environ['JIMSPORTS_API_KEY']
SHOP_TOK  = os.environ['SHOPIFY_TOKEN']
STORE     = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
APIV      = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
DRY       = (os.environ.get('DRY_RUN') or 'true').strip().lower() == 'true'
LIMIT     = int(os.environ.get('LIMIT') or '0')
ONLY_TITLE= (os.environ.get('ONLY_TITLE') or '').strip().lower()

JIM_BASE  = 'https://api.jimsports.com/v1'
SHOP_GQL  = f'https://{STORE}/admin/api/{APIV}/graphql.json'
HJ = {'ClientAuth': JIM_KEY, 'Accept': 'application/json', 'User-Agent': 'Komunika-FixVariants/1.0'}
HS = {'X-Shopify-Access-Token': SHOP_TOK, 'Content-Type': 'application/json'}
jsess = requests.Session(); jsess.headers.update(HJ)

# attribute_id de Jim -> nom d'opció Shopify (només dimensions de cara al comprador)
ATTR_OPT = {34:'Color', 21:'Talla calzado', 29:'Talla textil', 87:'Diseño',
            22:'Número', 3:'Tamaño', 13:'Talla balón', 6:'Talla calcetín',
            11:'Peso', 41:'Pulgadas', 92:'Tejido', 79:'Talla y lado',
            32:'Medidas', 24:'Medida', 76:'Densidad', 77:'Medida y densidad',
            72:'Fenólico', 33:'Modelo'}
# atributs descriptius que NO han de ser opcions
ATTR_SKIP = {16, 5, 8, 46, 52}  # Categoría, Cajas, Cantidades, Palas, Overgrips
ORDER = ['Color','Talla calzado','Talla textil','Talla balón','Talla calcetín',
         'Número','Tamaño','Talla y lado','Pulgadas','Medida','Medidas',
         'Densidad','Medida y densidad','Peso','Tejido','Fenólico','Diseño','Modelo']

VAL_ATTR = {}  # id -> (attribute_id, value_name_es)


def jim(ep, retries=3, timeout=20):
    for a in range(retries):
        try:
            r = jsess.get(f'{JIM_BASE}/{ep}', timeout=timeout)
            if r.status_code == 429:
                time.sleep(6); continue
            r.raise_for_status(); return r.json()
        except Exception:
            if a < retries - 1: time.sleep(2)
            else: return None
    return None


def gql(q, v=None):
    for _ in range(5):
        try:
            r = requests.post(SHOP_GQL, headers=HS, json={'query': q, 'variables': v or {}}, timeout=40)
            if r.status_code == 429:
                time.sleep(5); continue
            return r.json()
        except Exception:
            time.sleep(3)
    return None


# ─── ÍNDEX JIM: {ean: [attribute_value_id, ...]} ──────────────────────────────

def build_jim_index():
    global VAL_ATTR
    avs = jim('attribute_values') or []
    VAL_ATTR = {v['id']: (v.get('attribute_id'),
                          (v.get('name', {}).get('es-ES') or v.get('name', {}).get('en-US')))
                for v in avs}
    print(f'{len(VAL_ATTR)} attribute_values carregats', flush=True)

    ids = jim('products', timeout=60) or []
    print(f'{len(ids)} productes Jim a indexar', flush=True)
    index = {}; lock = threading.Lock(); cnt = [0]; failed = []

    def work(pid, total):
        p = jim(f'product/{pid}')
        if p is None:
            with lock: failed.append(pid); cnt[0] += 1
            return
        res = {}
        for v in (p.get('variants') or []):
            ean = v.get('ean13')
            if ean:
                res[str(ean)] = v.get('attribute_value') or []
        with lock:
            index.update(res); cnt[0] += 1
            if cnt[0] % 300 == 0:
                print(f'  index {cnt[0]}/{total} | {len(index)} EANs | {len(failed)} fallits', flush=True)

    # passada principal + reintents dels fallits (fins a 3 passades)
    pending = list(ids)
    for round_no in range(4):
        if not pending: break
        if round_no > 0:
            print(f'  reintent {round_no}: {len(pending)} productes fallits', flush=True)
            failed.clear()
        with ThreadPoolExecutor(max_workers=14) as ex:
            list(ex.map(lambda pid: work(pid, len(ids)), pending))
        pending = list(failed)
        if round_no > 0: time.sleep(5)
    print(f'Índex Jim llest: {len(index)} EANs | {len(pending)} productes irrecuperables', flush=True)
    return index


# ─── SHOPIFY ──────────────────────────────────────────────────────────────────

SCAN_Q = '''query($c:String){ products(first:100, after:$c, query:"tag:jimsports") {
  edges{ node{ id title variantsCount{count} } }
  pageInfo{hasNextPage endCursor} } }'''

def scan_products():
    out = []; c = None
    while True:
        d = gql(SCAN_Q, {'c': c})
        pr = d['data']['products']
        for e in pr['edges']:
            n = e['node']
            if n['variantsCount']['count'] <= 1: continue
            if ONLY_TITLE and ONLY_TITLE not in n['title'].lower(): continue
            out.append({'id': n['id'], 'title': n['title']})
        if not pr['pageInfo']['hasNextPage']: break
        c = pr['pageInfo']['endCursor']
    return out


PROD_Q = '''query($id:ID!,$c:String){ product(id:$id){ title options{name values}
  variants(first:250,after:$c){ edges{node{id barcode}} pageInfo{hasNextPage endCursor} } } }'''

def fetch_variants(gid):
    vs = []; c = None; title = ''; opts = []
    while True:
        d = gql(PROD_Q, {'id': gid, 'c': c})
        p = d['data']['product']
        title = p['title']; opts = p['options']
        for e in p['variants']['edges']: vs.append(e['node'])
        if not p['variants']['pageInfo']['hasNextPage']: break
        c = p['variants']['pageInfo']['endCursor']
    return title, opts, vs


PSET = '''mutation productSet($input:ProductSetInput!,$identifier:ProductSetIdentifiers!){
  productSet(input:$input,synchronous:true,identifier:$identifier){
    product{id} userErrors{field message} } }'''


def plan(gid, index):
    title, opts, vs = fetch_variants(gid)
    rows = []; missing = 0; unknown = 0
    for v in vs:
        ean = str(v.get('barcode') or '').strip()
        avids = index.get(ean)
        if avids is None:
            missing += 1; rows.append((v, None)); continue
        d = {}
        for i in avids:
            aid, name = VAL_ATTR.get(i, (None, None))
            if name is None: continue
            if aid in ATTR_SKIP: continue
            on = ATTR_OPT.get(aid)
            if on is None:
                continue  # atribut no contemplat -> ignora (no bloqueja)
            d[on] = name
        rows.append((v, d))
    if missing:
        return title, None, f'{missing}/{len(vs)} variants sense EAN a Jim'
    decoded = [(v, d) for v, d in rows if d]
    if not decoded:
        return title, None, 'cap atribut decodificat'

    # atributs presents a TOTES les variants
    counts = defaultdict(set)
    for _, d in decoded:
        for k, val in d.items(): counts[k].add(val)
    in_all = [k for k in counts if all(k in d for _, d in decoded)]
    # opcions: les que varien (>=2 valors) + Color sempre si hi és; ordenades; max 3
    chosen = [k for k in ORDER if k in in_all and (len(counts[k]) >= 2 or k == 'Color')]
    chosen += [k for k in in_all if k not in chosen and len(counts[k]) >= 2 and k not in ORDER]
    if not chosen and 'Color' not in in_all and in_all:
        chosen = in_all[:1]
    chosen = chosen[:3]
    if not chosen:
        return title, None, 'cap dimensió d opció clara'

    # SEGURETAT: no reduir dimensions reals sense revisar.
    # Si ara hi ha N opcions amb >=2 valors i la reconstrucció en deixaria menys, SALTA.
    cur_multi = sum(1 for o in opts if len(o['values']) >= 2)
    new_multi = sum(1 for k in chosen if len(counts[k]) >= 2)
    if new_multi < cur_multi:
        return title, None, f'perdria dimensió ({cur_multi} opcions amb >=2 valors -> {new_multi}) (revisió manual)'

    # cada variant ha de tenir totes les opcions triades
    for _, d in decoded:
        if any(k not in d for k in chosen):
            return title, None, f'variant incompleta (falta {[k for k in chosen if k not in d]})'

    optvals = {k: [] for k in chosen}
    seen = set(); vin = []
    for v, d in decoded:
        combo = tuple(d[k] for k in chosen)
        c2 = combo; idx = 2
        while c2 in seen:
            c2 = combo[:-1] + (f'{combo[-1]} ({idx})',); idx += 1
        seen.add(c2)
        for k, val in zip(chosen, c2):
            if val not in optvals[k]: optvals[k].append(val)
        vin.append({'id': v['id'], 'optionValues': [{'name': val, 'optionName': k}
                                                     for k, val in zip(chosen, c2)]})
    variables = {'identifier': {'id': gid}, 'input': {
        'productOptions': [{'name': k, 'values': [{'name': x} for x in optvals[k]]} for k in chosen],
        'variants': vin}}
    cur = [(o['name'], len(o['values'])) for o in opts]
    new = [(k, len(optvals[k])) for k in chosen]
    return title, variables, f'{cur} -> {new}'


def main():
    print(f'=== FIX VARIANT OPTIONS ({"DRY" if DRY else "LIVE"}) · store {STORE} ===', flush=True)
    index = build_jim_index()
    targets = scan_products()
    if LIMIT: targets = targets[:LIMIT]
    print(f'{len(targets)} productes multi-variant a revisar\n', flush=True)

    ok = skip = err = 0
    skips = []
    for i, p in enumerate(targets, 1):
        try:
            title, variables, msg = plan(p['id'], index)
        except Exception as e:
            skip += 1; skips.append(f'{p["title"][:45]} :: EXCEPTION {e}'); continue
        if variables is None:
            skip += 1; skips.append(f'{title[:45]} :: {msg}')
            continue
        if DRY:
            ok += 1; print(f'  [DRY {i}] {title[:45]}: {msg}', flush=True)
            continue
        d = gql(PSET, variables)
        ue = (d or {}).get('data', {}).get('productSet', {}).get('userErrors', [])
        if ue:
            err += 1; print(f'  ERR {title[:45]}: {ue[:2]}', flush=True)
        else:
            ok += 1; print(f'  OK {title[:45]}: {msg}', flush=True)
        time.sleep(0.35)

    print(f'\n=== RESUM: {ok} {"a arreglar" if DRY else "arreglats"}, {skip} saltats, {err} errors ===', flush=True)
    if skips:
        print(f'\n--- {len(skips)} SALTATS (revisió manual) ---', flush=True)
        for s in skips: print('  ·', s, flush=True)
    if DRY:
        print('\n(DRY_RUN actiu — no s\'ha modificat res. Posa DRY_RUN=false per aplicar.)', flush=True)


if __name__ == '__main__':
    main()
