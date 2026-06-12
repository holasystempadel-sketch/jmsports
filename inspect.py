import os, requests
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/diag'}
def g(ep):
    r=requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30)
    ct=r.headers.get('content-type','')
    try:
        return r.status_code,(r.json() if 'json' in ct else r.text[:300])
    except Exception as e:
        return r.status_code,'ERR '+str(e)
def show(ep):
    print('=====',ep)
    sc,p=g(ep);print('HTTP',sc)
    if isinstance(p,dict):
        for k in ('reference','ean13','discontinued','active','brand_id','category_ids','stock','price'):
            print('  ',k,'=',p.get(k))
        print('   name=',(p.get('name') or {}).get('es-ES'))
        vs=p.get('variants') or []
        print('   variants=',len(vs))
        for v in vs[:15]:
            print('     v',v.get('reference'),'ean=',v.get('ean13'),'disc=',v.get('discontinued'),'stock=',v.get('stock'))
    else:
        print('  ',p)
for ep in ('product/byref/A006647','product/77540','product/byref/77540'):
    show(ep)
sc,ids=g('products')
if isinstance(ids,list):
    print('TOTAL feed',len(ids),'| 77540 in feed:', str(77540) in set(str(x) for x in ids))
