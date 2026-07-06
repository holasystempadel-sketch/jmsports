"""
Patch v2 del bloqueig del cataleg professional (theme.liquid) + boto home:

  1. Protegeix TOTES les col-leccions (presents i futures), no una llista fixa.
  2. Protegeix la pagina 404 (mostrava productes recomanats sense codi).
  3. El codi d'acces caduca: passades 4 hores (14400 s) es torna a demanar.
  4. El boto "Acceso a tienda profesional" de la home passa de
     /collections/profesionales (404) a /pages/tienda-online.

Idempotent. Fa backup dels dos fitxers.

Env: SHOPIFY_TOKEN, SHOPIFY_STORE, THEME_ID
"""
import os
import json
import requests

TOKEN    = os.environ['SHOPIFY_TOKEN']
STORE    = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API      = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
THEME_ID = os.environ['THEME_ID']

URL = f'https://{STORE}/admin/api/{API}/graphql.json'
H = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}
GID = f'gid://shopify/OnlineStoreTheme/{THEME_ID}'


def gql(query, variables):
    r = requests.post(URL, headers=H, json={'query': query, 'variables': variables}, timeout=60)
    j = r.json()
    if j.get('errors'):
        print('ERRORS:', json.dumps(j['errors'])[:500])
        raise SystemExit(1)
    return j


def get_file(key):
    q = '''query($id: ID!, $names: [String!]!) {
      theme(id: $id) { files(filenames: $names, first: 1) {
        nodes { body { ... on OnlineStoreThemeFileBodyText { content } } } } }
    }'''
    j = gql(q, {'id': GID, 'names': [key]})
    nodes = j['data']['theme']['files']['nodes']
    if not nodes:
        print(f'ERROR: no es troba {key}')
        raise SystemExit(1)
    return nodes[0]['body']['content']


def put_file(key, content):
    m = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
      themeFilesUpsert(themeId: $themeId, files: $files) {
        upsertedThemeFiles { filename } userErrors { field message } }
    }'''
    j = gql(m, {'themeId': GID, 'files': [{'filename': key, 'body': {'type': 'TEXT', 'value': content}}]})
    res = j['data']['themeFilesUpsert']
    if res['userErrors']:
        print('USER ERRORS:', res['userErrors'])
        raise SystemExit(1)
    print(f'OK: {key} actualitzat')


# ============ 1. theme.liquid ============
KEY = 'layout/theme.liquid'
content = get_file(KEY)
print(f'{KEY}: {len(content)} chars')
with open('backup_theme_liquid.liquid', 'w') as f:
    f.write(content)

if 'sp_access_ts' in content:
    print('El patch gate v2 ja hi es. Salto theme.liquid.')
else:
    OLD_COLLECTIONS = """      if template contains 'collection'
        assign jim_handles = 'padel,entrenamiento,r-p,deportes,textil-y-calzado,palas-de-padel,paleteros-y-mochilas,pelotas-de-padel,accesorios-padel,textil-padel,accesorios-entrenamiento,psicomotricidad,complementos-entrenamiento,palas-r-p,raquetas,pelotas-r-p,deportes-de-equipo,deportes-individual,deportes-de-raqueta,natacion,fitness,juegos,calzado,textil-promocional,linea-work,equipaciones,equipamiento,novedades,outlet' | split: ','
        if jim_handles contains collection.handle
          assign sp_protect = true
        endif
      endif"""
    NEW_COLLECTIONS = """      if template contains 'collection'
        assign sp_protect = true
      endif
      if template contains '404'
        assign sp_protect = true
      endif"""

    OLD_ACCESS = """      assign sp_has_access = false
      if cart.attributes.sp_access == 'systempadel2026'
        assign sp_has_access = true
      endif"""
    NEW_ACCESS = """      assign sp_has_access = false
      if cart.attributes.sp_access == 'systempadel2026'
        assign sp_now = 'now' | date: '%s' | plus: 0
        assign sp_ts = cart.attributes.sp_access_ts | plus: 0
        assign sp_age = sp_now | minus: sp_ts
        if sp_ts > 0 and sp_age < 14400
          assign sp_has_access = true
        endif
      endif"""

    OLD_JS = "body: JSON.stringify({ attributes: { sp_access: 'systempadel2026' } })"
    NEW_JS = "body: JSON.stringify({ attributes: { sp_access: 'systempadel2026', sp_access_ts: String(Math.floor(Date.now()/1000)) } })"

    for old, new, tag in [(OLD_COLLECTIONS, NEW_COLLECTIONS, 'col-leccions'),
                          (OLD_ACCESS, NEW_ACCESS, 'caducitat'),
                          (OLD_JS, NEW_JS, 'js')]:
        n = content.count(old)
        if n != 1:
            print(f'ERROR: ancoratge "{tag}" trobat {n} cops (esperat 1)')
            raise SystemExit(1)
    for old, new, tag in [(OLD_COLLECTIONS, NEW_COLLECTIONS, 'col-leccions'),
                          (OLD_ACCESS, NEW_ACCESS, 'caducitat'),
                          (OLD_JS, NEW_JS, 'js')]:
        content = content.replace(old, new, 1)
    print('3 substitucions aplicades a theme.liquid')
    put_file(KEY, content)

# ============ 2. boto home (templates/index.json) ============
KEY2 = 'templates/index.json'
content2 = get_file(KEY2)
with open('backup_index.json', 'w') as f:
    f.write(content2)

if '/collections/profesionales' not in content2:
    print('Cap referencia a /collections/profesionales a index.json. Res a fer.')
else:
    n = content2.count('/collections/profesionales')
    content2 = content2.replace('/collections/profesionales', '/pages/tienda-online')
    print(f'{n} enllac(os) del boto home corregits a index.json')
    put_file(KEY2, content2)

print('PATCH GATE V2 COMPLET')
