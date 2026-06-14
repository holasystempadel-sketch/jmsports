import os, requests, json
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/flag'}
def g(ep):
    return requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30).json()
def info(p):
    nm=(p.get('name') or {}).get('es-ES') or ''
    return dict(ref=p.get('reference'), name=nm[:42], web=p.get('web'), on_demand=p.get('on_demand'), disc=p.get('discontinued'), stock=p.get('stock'), cats=p.get('category_ids'))
for ref in ['A005504','A006647']:
    print('KNOWN', json.dumps(info(g('product/byref/'+ref)), ensure_ascii=False))
feed=g('products')
print('feed len', len(feed))
found=0; it=0
for pid in reversed(feed):
    it+=1
    if it>800: break
    try:
        p=g('product/'+str(pid))
    except Exception:
        continue
    nm=(p.get('name') or {}).get('es-ES') or ''
    if 'GLOBAL' in nm.upper():
        print('GLOBAL', json.dumps(info(p), ensure_ascii=False))
        found+=1
        if found>=6: break
print('done it=',it,'found=',found)
