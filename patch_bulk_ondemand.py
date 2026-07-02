"""
Patch de sections/main-product-bulk.liquid al tema indicat:
els productes amb tag `bajo-demanda` mostren "Bajo pedido" en lloc
d'"Agotado" (taula de variants, fila Stock lateral) i, si no tenen
stock, un CTA de consulta en lloc del formulari.

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


def gql(query, variables):
    r = requests.post(URL, headers=H, json={'query': query, 'variables': variables}, timeout=60)
    print('HTTP', r.status_code)
    j = r.json()
    if j.get('errors'):
        print('ERRORS:', json.dumps(j['errors'])[:500])
        raise SystemExit(1)
    return j


# 1. Llegir contingut actual
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

# 2. Idempotencia
if 'systempadel-product__on-demand' in content:
    print('El patch bajo-demanda ja hi es. Res a fer.')
    raise SystemExit(0)

# 3. Substitucions quirurgiques (cadascuna ha d'apareixer EXACTAMENT 1 cop)
REPLACEMENTS = [
    # a) assignar la variable bajo_demanda
    ("{%- assign consultar = false -%}",
     "{%- assign consultar = false -%}\n"
     "        {%- assign bajo_demanda = false -%}\n"
     "        {%- if product.tags contains 'bajo-demanda' -%}{%- assign bajo_demanda = true -%}{%- endif -%}"),
    # b) fila Stock del lateral
    ("<td>{{ total_stock }} ud(s).</td>",
     "<td>{%- if bajo_demanda and total_stock == 0 -%}Bajo pedido{%- else -%}{{ total_stock }} ud(s).{%- endif -%}</td>"),
    # c) celda de la taula de variants
    ('<span class="systempadel-product__sold-out">Agotado</span>',
     '{%- if bajo_demanda -%}<span class="systempadel-product__on-demand">Bajo pedido</span>'
     '{%- else -%}<span class="systempadel-product__sold-out">Agotado</span>{%- endif -%}'),
    # d) CTA de consulta quan es bajo-demanda i sense stock (abans de la branca matrix)
    ("{%- elsif product.variants.size > 1 -%}",
     "{%- elsif bajo_demanda and total_stock == 0 -%}\n"
     "      <div class=\"systempadel-product__simple\">\n"
     "        <p>Producto fabricado bajo pedido. Solic\u00edtanos plazo de entrega y disponibilidad sin compromiso.</p>\n"
     "        <a class=\"systempadel-btn systempadel-btn--primary\" href=\"/pages/contact\">Consultar disponibilidad</a>\n"
     "      </div>\n"
     "    {%- elsif product.variants.size > 1 -%}"),
    # e) estil del nou estat (blau, no vermell)
    (".systempadel-product__sold-out { color: var(--sp-danger); font-weight: 700; font-size: 12px; text-transform: uppercase; }",
     ".systempadel-product__sold-out { color: var(--sp-danger); font-weight: 700; font-size: 12px; text-transform: uppercase; }\n"
     "  .systempadel-product__on-demand { color: #2a5a72; font-weight: 700; font-size: 12px; text-transform: uppercase; }"),
]

for old, new in REPLACEMENTS:
    n = content.count(old)
    if n != 1:
        print(f'ERROR: ancoratge trobat {n} cops (esperat 1): {old[:60]}...')
        raise SystemExit(1)

for old, new in REPLACEMENTS:
    content = content.replace(old, new, 1)
print('5 substitucions aplicades')

# 4. Upsert
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
print('OK: patch bajo-demanda aplicat a', res['upsertedThemeFiles'][0]['filename'])
