import os, requests, json, collections
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/webcount'}
def g(ep):
    return requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30).json()
feed=g('products')
print('feed total', len(feed))
cnt=collections.Counter(); falses=[]
N=150
for pid in feed[:N]:
    try:
        p=g('product/'+str(pid))
    except Exception:
        continue
    w=bool(p.get('web')); d=bool(p.get('discontinued'))
    cnt[(w,d)]+=1
    if not w:
        nm=(p.get('name') or {}).get('es-ES') or ''
        falses.append(nm[:42])
print('sample N=',N,' counts (web,disc):', dict(cnt))
print('web-false count in sample:', len(falses))
print('web-false names:', falses[:20])
