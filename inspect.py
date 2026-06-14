import os, requests, json
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/ean'}
def g(ep):
    r=requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30)
    return r.status_code, r.text
def web(ref):
    sc,t=g('product/byref/'+ref)
    try:
        p=json.loads(t); return dict(ref=ref, web=p.get('web'), on_demand=p.get('on_demand'), disc=p.get('discontinued'))
    except Exception:
        return (sc, t[:100])
print('KNOWN A005504', web('A005504'))
print('KNOWN A006647', web('A006647'))
for ean in ['8445090162016','8445090160760']:
    for ep in ['product/byean/'+ean,'product/byean13/'+ean,'products?ean13='+ean,'product/ean/'+ean]:
        sc,t=g(ep)
        print(ep, sc, t[:120].replace(chr(10),' '))
