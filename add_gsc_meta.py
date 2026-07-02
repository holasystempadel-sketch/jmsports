"""
Afegeix la metaetiqueta de verificacio de Google Search Console al
layout/theme.liquid del tema indicat, just despres de la verificacio
existent. Idempotent: si el token ja hi es, no fa res.

Env: SHOPIFY_TOKEN, SHOPIFY_STORE, THEME_ID, GSC_TOKEN
"""
import os
import json
import requests

TOKEN     = os.environ['SHOPIFY_TOKEN']
STORE     = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API       = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
THEME_ID  = os.environ['THEME_ID']
GSC_TOKEN = os.environ['GSC_TOKEN']
KEY       = 'layout/theme.liquid'

URL = f'https://{STORE}/admin/api/{API}/graphql.json'
H = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}
GID = f'gid://shopify/OnlineStoreTheme/{THEME_ID}'

NEW_META = f'<meta name="google-site-verification" content="{GSC_TOKEN}" />'


def gql(query, variables):
    r = requests.post(URL, headers=H, json={'query': query, 'variables': variables}, timeout=60)
    print('HTTP', r.status_code)
    j = r.json()
    if j.get('errors'):
        print('ERRORS:', json.dumps(j['errors'])[:500])
        raise SystemExit(1)
    return j


# 1. Llegir el contingut actual
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

# Backup (artifact)
with open('backup_theme_file.liquid', 'w') as f:
    f.write(content)

# 2. Idempotencia
if GSC_TOKEN in content:
    print('El token GSC ja hi es. Res a fer.')
    raise SystemExit(0)

# 3. Inserir despres de la verificacio existent (o despres de <head> si no n'hi ha)
anchor = None
for line in content.split('\n'):
    if 'google-site-verification' in line:
        anchor = line
        break
if anchor:
    content2 = content.replace(anchor, anchor + '\n    ' + NEW_META, 1)
    print('Inserit despres de la verificacio existent')
else:
    content2 = content.replace('<head>', '<head>\n    ' + NEW_META, 1)
    print('Inserit despres de <head>')

assert NEW_META in content2 and len(content2) == len(content) + len(NEW_META) + 5

# 4. Upsert
m = '''mutation($themeId: ID!, $files: [OnlineStoreThemeFilesUpsertFileInput!]!) {
  themeFilesUpsert(themeId: $themeId, files: $files) {
    upsertedThemeFiles { filename }
    userErrors { field message }
  }
}'''
j = gql(m, {'themeId': GID, 'files': [{'filename': KEY, 'body': {'type': 'TEXT', 'value': content2}}]})
res = j['data']['themeFilesUpsert']
if res['userErrors']:
    print('USER ERRORS:', res['userErrors'])
    raise SystemExit(1)
print('OK: meta GSC afegida a', res['upsertedThemeFiles'][0]['filename'])
