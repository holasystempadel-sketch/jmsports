import os, requests, json
KEY=os.environ['JIMSPORTS_API_KEY']
H={'ClientAuth':KEY,'Accept':'application/json','User-Agent':'SP/full'}
def g(ep):
    return requests.get('https://api.jimsports.com/v1/'+ep,headers=H,timeout=30).json()
feed=g('products')
print('FEED type', type(feed).__name__, 'len', len(feed))
print('FEED[0]:', json.dumps(feed[0])[:200])
for ref in ['A005504','A006647']:
    p=g('product/byref/'+ref)
    print('===== '+ref+' KEYS:', list(p.keys()))
    print(json.dumps(p, ensure_ascii=False)[:1800])
