"""
Patch de sections/main-product-bulk.liquid: la fila REFERENCIA del lateral
mostra la referencia real de Jim (part del SKU abans del primer punt,
ex. 24190.003.1 -> 24190) en lloc del handle en majuscules.

Idempotent: si el patch ja hi es, no fa res.

Env: SHOPIFY_TOKEN, SHOPIFY_STORE, THEME_ID
"""
import os
import json
import requests

TOKEN    = os.environ['SHOPIFY_TOKEN']
STORE    = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API      = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
THEME_ID = os.environ['THEME_ID']
KEY      = 'sections/main-product-bulk.liquid'

URL = f'https://{STORE}/admin/api/{API}/graphql.json'
H = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}
GID = f'gid://shopify/OnlineStoreTheme/{THEME_ID}'

OLD = "assign product_ref = product.metafields.custom.reference | default: product.handle | replace: 'jimsports-', '' | upcase"
NEW = ("assign product_ref = product.variants.first.sku | split: '.' | first | upcase\n"
       "  if product_ref == blank\n"
       "    assign product_ref = product.handle | replace: 'jimsports-', '' | upcase\n"
       "  endif")


def gql(query, variables):
    r = requests.post(URL, headers=H, json={'query': query, 'variables': variables}, timeout=60)
    print('HTTP', r.status_code)
    j = r.json()
    if j.get('errors'):
        print('ERRORS:', json.dumps(j['errors'])[:500])
        raise SystemExit(1)
    return j


q = '''query($id: ID!, $names: [String!]!) {
  theme(id: $id) {
    files(filenames: $names, first: 1) {
      nodes { body { ... on OnlineStoreThemeFileBodyText { content } } }
    }
  }
}'''
j = gql(q, {'id': GID, 'names': [KEY]})
nodes = j['data']['theme']['files']['nodes']
if not nodes:
    print(f'ERROR: no es troba {KEY}')
    raise SystemExit(1)
content = nodes[0]['body']['content']
print(f'{KEY}: {len(content)} chars llegits')

with open('backup_theme_file.liquid', 'w') as f:
    f.write(content)

if "product.variants.first.sku | split: '.'" in content:
    print('El patch REFERENCIA ja hi es. Res a fer.')
    raise SystemExit(0)

n = content.count(OLD)
if n != 1:
    print(f'ERROR: ancoratge trobat {n} cops (esperat 1)')
    raise SystemExit(1)

content = content.replace(OLD, NEW, 1)
print('Substitucio aplicada')

m = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename }
    userErrors { field message }
  }
}'''
j = gql(m, {'themeId': GID, 'files': [{'filename': KEY, 'body': {'type': 'TEXT', 'value': content}}]})
res = j['data']['themeFilesUpsert']
if res['userErrors']:
    print('USER ERRORS:', res['userErrors'])
    raise SystemExit(1)
print('OK: patch REFERENCIA aplicat a', res['upsertedThemeFiles'][0]['filename'])
