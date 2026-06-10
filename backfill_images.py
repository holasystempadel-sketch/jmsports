"""
Backfill missing images on Shopify SystemPadel products from Jim Sports source URLs.

Reads media_to_add.json (1086 products, 5563 images).
For each product, calls productCreateMedia to add missing image URLs.

Env vars required:
  SHOPIFY_TOKEN  Admin API access token (from GitHub Secrets)
  SHOPIFY_STORE  defaults to 0dcf6c-2.myshopify.com
"""
import os, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request

SHOPIFY_TOKEN = os.environ['SHOPIFY_TOKEN']
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '0dcf6c-2.myshopify.com')
API_VERSION = '2025-01'
GQL_URL = f'https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json'

MUTATION = '''
mutation AddMedia($productId: ID!, $media: [CreateMediaInput!]!) {
  productCreateMedia(productId: $productId, media: $media) {
    mediaUserErrors { field message }
  }
}
'''

def call_gql(payload, retries=4):
    body = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(GQL_URL, data=body, headers={
        'X-Shopify-Access-Token': SHOPIFY_TOKEN,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    })
    last_err = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                resp = json.loads(r.read())
                if 'errors' in resp:
                    last_err = resp['errors']
                    if any('THROTTLED' in str(e) for e in resp['errors']):
                        time.sleep(2 ** attempt)
                        continue
                    return resp
                return resp
        except Exception as e:
            last_err = str(e)
            time.sleep(1 + attempt)
    return {'errors': last_err}


def process_one(p):
    media = [{'originalSource': u, 'mediaContentType': 'IMAGE'} for u in p['missing']]
    payload = {
        'query': MUTATION,
        'variables': {'productId': p['gid'], 'media': media}
    }
    resp = call_gql(payload)
    errs = resp.get('data', {}).get('productCreateMedia', {}).get('mediaUserErrors', [])
    if errs:
        return p['ref'], False, str(errs)
    if 'errors' in resp:
        return p['ref'], False, str(resp['errors'])
    return p['ref'], True, None


def main():
    with open('media_to_add.json') as f:
        items = json.load(f)
    print(f'Backfill: {len(items)} products, {sum(len(p["missing"]) for p in items)} total images', flush=True)

    ok = fail = 0
    errors = []
    start = time.time()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(process_one, p): p for p in items}
        for i, f in enumerate(as_completed(futs), 1):
            ref, success, err = f.result()
            if success:
                ok += 1
            else:
                fail += 1
                errors.append((ref, err))
            if i % 50 == 0:
                rate = i / (time.time() - start)
                eta = (len(items) - i) / rate if rate > 0 else 0
                print(f'  {i}/{len(items)} ok={ok} fail={fail} rate={rate:.1f}/s eta={eta/60:.1f}min', flush=True)

    print(f'\nDONE: ok={ok} fail={fail} time={(time.time()-start)/60:.1f}min', flush=True)
    if errors:
        print('Sample errors:')
        for r, e in errors[:5]:
            print(f'  {r}: {e}')


if __name__ == '__main__':
    main()
