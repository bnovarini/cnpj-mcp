import os,sys,json,asyncio
from pathlib import Path
sys.path.insert(0,sys.argv[1]+'/src')
import cnpj_mcp.mcp_server as m
from starlette.testclient import TestClient
requests=[('dataset_info',{}),('search_companies',{'uf':'SP'}),('count_companies',{'group_by':['uf']}),('get_company',{'cnpj':'11111111000191'}),('list_partners',{'cnpj':'11111111'}),('search_partners',{'name':'Maria'}),('companies_at_address',{'cep':'01000000'}),('lookup_codes',{'kind':'cnae','text':'soja'})]
# Discover registered tools and compare actual HTTP JSON bodies, including error shapes.
names={t.name for t in m.mcp._tool_manager.list_tools()}
requests=[x for x in requests if x[0] in names]
if len(sys.argv)>2:
 os.environ['CNPJ_POSTHOG_TOKEN']='test_dummy_key'
 import requests as transport
 def offline(*a,**kw):
  r=transport.Response();r.status_code=200;r._content=b'{}';r.headers['Content-Length']='2';return r
 transport.Session.post=offline
app=m.http_app(); result=[]
with TestClient(app) as c:
 for i,(name,args) in enumerate(requests):
  r=c.post('/mcp',headers={'accept':'application/json, text/event-stream'},json={'jsonrpc':'2.0','id':i+1,'method':'tools/call','params':{'name':name,'arguments':args}})
  result.append([name,r.status_code,r.text])
print(json.dumps(result,separators=(',',':')))
