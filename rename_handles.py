"""
Renombra els handles jimsports-XXXX a slugs del titol del producte
i crea redirects 301 de la URL antiga a la nova.

Variables d'entorn:
  SHOPIFY_TOKEN  (secret)
  SHOPIFY_STORE  (defecte 'xqksc3-ua.myshopify.com')
  DRY_RUN        (defecte 'true' = nomes mostra el mapping, no canvia res)

Genera handles_map.json amb el mapping complet old -> new.
"""
import os
import re
import json
import time
import unicodedata
import requests

SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API_VERSION   = os.environ.get('SHOPIFY_API_VERSION', '2025-10')
DRY_RUN       = (os.environ.get('DRY_RUN') or 'true').strip().lower() == 'true'

BASE = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}'
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def req(method, url, data=None, retries=5):
    for attempt in range(retries):
        try:
            r = requests.request(method, url, headers=HEADERS, json=data, timeout=30)
            if r.status_code == 429:
                time.sleep(float(r.headers.get('Retry-After', 5)))
                continue
            return r
        except requests.exceptions.RequestException:
            if attempt < retries - 1:
                time.sleep(4)
    return None


def slugify(text):
    text = unicodedata.normalize('NFKD', text or '').encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')[:200]


def fetch_all_products():
    products = []
    url = f'{BASE}/products.json?limit=250&fields=id,title,handle'
    while url:
        r = req('GET', url)
        if not r or r.status_code >= 400:
            print(f'ERROR llegint productes: {r.status_code if r else "sense resposta"}')
            break
        products.extend(r.json().get('products', []))
        url = None
        for part in r.headers.get('Link', '').split(','):
            if 'rel="next"' in part:
                url = part.split(';')[0].strip().strip('<>')
                break
        time.sleep(0.3)
    return products


def create_redirect(old_handle, new_handle):
    """Retorna None si OK, o el missatge d'error."""
    q = {
        'query': 'mutation($r: UrlRedirectInput!){ urlRedirectCreate(urlRedirect:$r){ urlRedirect{id} userErrors{message} } }',
        'variables': {'r': {'path': f'/products/{old_handle}', 'target': f'/products/{new_handle}'}},
    }
    r = req('POST', f'{BASE}/graphql.json', data=q)
    if not r or r.status_code >= 400:
        return f'HTTP {r.status_code if r else "?"}'
    j = r.json()
    if j.get('errors'):
        return str(j['errors'])[:150]
    errs = (j.get('data') or {}).get('urlRedirectCreate', {}).get('userErrors') or []
    if errs:
        return errs[0].get('message', 'userError')[:150]
    return None


def main():
    products = fetch_all_products()
    print(f'{len(products)} productes totals a la botiga')
    targets = [p for p in products if (p.get('handle') or '').startswith('jimsports-')]
    targets.sort(key=lambda p: p['id'])
    print(f'{len(targets)} amb handle jimsports-*  ·  DRY_RUN={DRY_RUN}\n')

    used = {p['handle'] for p in products}
    mapping = []
    renamed = rename_fail = redirect_fail = 0

    for i, p in enumerate(targets, 1):
        old = p['handle']
        ref = old[len('jimsports-'):]
        slug = slugify(p.get('title') or '') or f'producto-{slugify(ref)}'
        new = slug
        if new in used:
            new = f'{slug}-{slugify(ref)}'
        base, n = new, 2
        while new in used:
            new = f'{base}-{n}'
            n += 1
        used.add(new)
        mapping.append({'id': p['id'], 'old': old, 'new': new, 'title': p.get('title')})

        if DRY_RUN:
            if i <= 30:
                print(f'  {old}  ->  {new}')
            continue

        r = req('PUT', f'{BASE}/products/{p["id"]}.json',
                data={'product': {'id': p['id'], 'handle': new}})
        if not r or r.status_code >= 400:
            rename_fail += 1
            print(f'  [{i}] ERROR renombrant {old}: HTTP {r.status_code if r else "?"} {(r.text[:150] if r else "")}')
            time.sleep(0.55)
            continue
        renamed += 1

        err = create_redirect(old, new)
        if err:
            redirect_fail += 1
            if redirect_fail <= 5:
                print(f'  [{i}] redirect KO ({old}): {err}')

        if i % 100 == 0:
            print(f'  [{i}/{len(targets)}] {renamed} renombrats · {rename_fail} errors · {redirect_fail} redirects KO')
        time.sleep(0.55)

    with open('handles_map.json', 'w') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=1)

    print('\n=== RESUM ===')
    print(f'Candidats:        {len(targets)}')
    print(f'Renombrats:       {renamed}')
    print(f'Errors rename:    {rename_fail}')
    print(f'Redirects fallits: {redirect_fail}')
    print('Mapping complet a handles_map.json (artifact del workflow)')
    if DRY_RUN:
        print('\nDRY RUN: no s\'ha canviat res. Torna a executar amb dry_run=false per aplicar.')


if __name__ == '__main__':
    main()
