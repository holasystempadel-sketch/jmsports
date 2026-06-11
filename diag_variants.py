"""
Diagnòstic del model d'atributs de Jim Sports per dissenyar el merge de variants.
Imprimeix:
  1) Estructura de /v1/attributes  (grups: Color, Talla, Estampado, ...)
  2) Mostra de /v1/attribute_values amb el seu attribute_id (quin grup)
  3) Uns quants productes amb MÚLTIPLES variants: la referència de cada variant,
     descomposada en parts, i a quin atribut pertany cada part.

Només LECTURA. No escriu res a Shopify ni a Jim.
Variables d'entorn: JIMSPORTS_API_KEY (secret).
"""
import os, re, json, requests

KEY = os.environ['JIMSPORTS_API_KEY']
H = {'ClientAuth': KEY, 'Accept': 'application/json', 'User-Agent': 'SP-Diag/1.0'}
SAMPLE_PRODUCTS = int(os.environ.get('SAMPLE_PRODUCTS') or '12')


def jim(ep):
    r = requests.get(f'https://api.jimsports.com/v1/{ep}', headers=H, timeout=30)
    r.raise_for_status()
    return r.json()


def name_of(obj):
    n = obj.get('name')
    if isinstance(n, dict):
        return n.get('es-ES') or n.get('en-US') or str(obj.get('id'))
    return n or str(obj.get('id'))


print('=' * 70)
print('1) /v1/attributes')
print('=' * 70)
attrs = jim('attributes') or []
print(f'{len(attrs)} attributes')
print('SAMPLE RAW (first 3):', json.dumps(attrs[:3], ensure_ascii=False)[:1500])
attr_name = {}
for a in attrs:
    attr_name[a['id']] = name_of(a)
print('ATTR IDs -> name:', json.dumps(attr_name, ensure_ascii=False)[:2000])

print()
print('=' * 70)
print('2) /v1/attribute_values  (com es lliga value -> attribute)')
print('=' * 70)
avs = jim('attribute_values') or []
print(f'{len(avs)} attribute_values')
print('SAMPLE RAW (first 5):', json.dumps(avs[:5], ensure_ascii=False)[:2000])
# map value_id -> (attribute_id, value_name)
val_attr = {}
for v in avs:
    aid = v.get('attribute_id') or v.get('id_attribute') or v.get('attribute')
    val_attr[v['id']] = (aid, name_of(v))
# distribució per atribut
from collections import Counter
c = Counter(attr_name.get(val_attr[k][0], str(val_attr[k][0])) for k in val_attr)
print('valors per atribut:', dict(c))

print()
print('=' * 70)
print('3) Productes amb múltiples variants (mostra)')
print('=' * 70)
ids = jim('products') or []
shown = 0
dumped = 0
for pid in ids:
    if shown >= SAMPLE_PRODUCTS:
        break
    p = jim(f'product/{pid}')
    if not p:
        continue
    variants = [v for v in (p.get('variants') or []) if not v.get('discontinued')]
    if len(variants) <= 1:
        continue
    # bolca el JSON SENCER dels 2 primers productes multi-variant per veure l'estructura
    if dumped < 2:
        print('\n##### RAW PRODUCT JSON #####')
        print('  product keys:', list(p.keys()))
        print('  variant[0] keys:', list(variants[0].keys()))
        print(json.dumps(p, ensure_ascii=False)[:4500])
        print('##### /RAW #####')
        dumped += 1
    base = p.get('reference', '')
    nm = name_of(p)
    print(f'\n--- {base}  "{nm[:50]}"  ({len(variants)} variants) ---')
    for v in variants[:6]:
        ref = v.get('reference', '')
        suffix = ref[len(base):].lstrip('.') if ref.startswith(base) else ref
        parts = re.split(r'[.\-_]', suffix)
        decoded = []
        for part in parts:
            if not part:
                continue
            try:
                vid = int(part)
                aid, vname = val_attr.get(vid, (None, part))
                decoded.append(f'{attr_name.get(aid, aid)}={vname}')
            except ValueError:
                decoded.append(f'?={part}')
        print(f'   {ref}  ->  ' + ' | '.join(decoded))
    shown += 1

print('\n=== FI DIAGNÒSTIC ===')
