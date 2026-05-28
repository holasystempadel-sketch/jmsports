"""
Esborra TOTS els productes de Shopify que tenen el tag `jimsports`.
Per executar abans del sync v2 complet i deixar la botiga neta.

Variables d'entorn:
  SHOPIFY_TOKEN  (secret)
  SHOPIFY_STORE  (defecte 'xqksc3-ua.myshopify.com')
"""
import os
import time
import requests

SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'xqksc3-ua.myshopify.com')
API_VERSION   = os.environ.get('SHOPIFY_API_VERSION', '2025-10')

BASE = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}'
H = {
    'X-Shopify-Access-Token': SHOPIFY_TOKEN,
    'Content-Type': 'application/json',
}


def req(method, endpoint, retries=5):
    url = endpoint if endpoint.startswith('http') else f'{BASE}/{endpoint}'
    for attempt in range(retries):
        r = requests.request(method, url, headers=H, timeout=30)
        if r.status_code == 429:
            wait = float(r.headers.get('Retry-After', 10))
            print(f'  Rate limit, esperant {wait}s')
            time.sleep(wait)
            continue
        if r.status_code in (404, 200, 201, 202, 204):
            return r
        if attempt < retries - 1:
            print(f'  HTTP {r.status_code}, reintent en 5s')
            time.sleep(5)
            continue
        print(f'  Fallida final: HTTP {r.status_code} - {r.text[:200]}')
        return r
    return None


def main():
    print('=== Esborrant productes jimsports de Shopify ===')
    print(f'Store: {SHOPIFY_STORE}')

    deleted = total_seen = 0
    page = 1

    while True:
        url = f'{BASE}/products.json?limit=250&fields=id,title,tags'
        r = req('GET', url)
        if not r or r.status_code >= 400:
            print('No es pot llegir productes, parem.')
            break

        products = r.json().get('products', [])
        if not products:
            break

        targets = [p for p in products
                   if 'jimsports' in (p.get('tags') or '').lower()]
        total_seen += len(products)

        if not targets:
            # No queden jimsports a aquesta pàgina i Shopify torna sempre des del 0
            # Comprovar si encara queden productes amb tag jimsports en total
            print(f'  Pàgina {page}: cap producte jimsports trobat. Mirem la següent...')
            # Si no n'hi ha cap als 250, probablement no en queden
            check = req('GET', f'{BASE}/products.json?limit=1&fields=id,tags')
            if check and check.status_code == 200:
                remaining = [p for p in check.json().get('products', [])
                             if 'jimsports' in (p.get('tags') or '').lower()]
                if not remaining:
                    break
            page += 1
            continue

        print(f'\n  Pàgina {page}: {len(targets)} productes jimsports a esborrar')

        for p in targets:
            r = req('DELETE', f'products/{p["id"]}.json')
            ok = r and r.status_code in (200, 204)
            if ok:
                deleted += 1
                if deleted % 25 == 0:
                    print(f'    {deleted} esborrats...')
            else:
                print(f'    ERROR amb {p["id"]} {p["title"][:50]}')
            time.sleep(0.25)

        page += 1

    print(f'\n=== ACABAT ===')
    print(f'Total esborrats: {deleted}')


if __name__ == '__main__':
    main()
