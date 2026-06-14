import os, requests, json
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/cats'}
def g(ep):
    r=requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30)
    return r.json()
cats=g('categories')
print('TOTAL categories:', len(cats))
print('SAMPLE:', json.dumps(cats[0], ensure_ascii=False)[:600])
def nm(c):
    n=c.get('name') or {}
    return n.get('es-ES') or n.get('en-US') or ''
def par(c):
    for k in ('parent_id','id_parent','parent','id_parent_category','parentId','parents'):
        if k in c: return (k, c[k])
    return None
for c in cats:
    print(c.get('id'), '|', nm(c), '|', par(c))
