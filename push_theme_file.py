"""
Puja un fitxer del repo al tema de Shopify via GraphQL themeFilesUpsert.
Fa backup previ del contingut actual (artifact).

Env: SHOPIFY_TOKEN, SHOPIFY_STORE, THEME_ID, SRC (ruta al repo), KEY (ruta al tema)
"""
import os
import json
import requests

TOKEN    = os.environ['SHOPIFY_TOKEN']
STORE    = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API      = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
THEME_ID = os.environ['THEME_ID']
SRC      = os.environ['SRC']
KEY      = os.environ['KEY']

URL = f'https://{STORE}/admin/api/{API}/graphql.json'
H = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}
GID = f'gid://shopify/OnlineStoreTheme/{THEME_ID}'


def gql(query, variables):
    r = requests.post(URL, headers=H, json={'query': query, 'variables': variables}, timeout=60)
    print('HTTP', r.status_code)
    j = r.json()
    if j.get('errors'):
        print('ERRORS:', json.dumps(j['errors'])[:500])
    return j


# 1. Backup del contingut actual
q = '''query($id: ID!, $names: [String!]!) {
  theme(id: $id) {
    files(filenames: $names, first: 1) {
      nodes { filename body { ... on OnlineStoreThemeFileBodyText { content } } }
    }
  }
}'''
j = gql(q, {'id': GID, 'names': [KEY]})
nodes = (((j.get('data') or {}).get('theme') or {}).get('files') or {}).get('nodes') or []
if nodes:
    old = (nodes[0].get('body') or {}).get('content') or ''
    with open('backup_theme_file.liquid', 'w') as f:
        f.write(old)
    print(f'Backup de {KEY}: {len(old)} chars')
else:
    open('backup_theme_file.liquid', 'w').write('')
    print(f'AVIS: no s\'ha pogut llegir {KEY} (potser no existeix o falta scope read_themes)')

# 2. Upsert del fitxer nou
content = open(SRC).read()
m = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename }
    userErrors { field message }
  }
}'''
j = gql(m, {'themeId': GID, 'files': [{'filename': KEY, 'body': {'type': 'TEXT', 'value': content}}]})
data = (j.get('data') or {}).get('themeFilesUpsert') or {}
errs = data.get('userErrors') or []
if errs:
    print('USER ERRORS:', json.dumps(errs)[:500])
    raise SystemExit(1)
print('UPSERT OK:', json.dumps(data.get('upsertedThemeFiles')))
print(f'{KEY} actualitzat ({len(content)} chars)')
