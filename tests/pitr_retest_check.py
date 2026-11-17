import json, sys, time, urllib.request
sys.path.insert(0, "/tmp/pylibs")
API="http://127.0.0.1:8000"
def call(method, path, body=None):
    req=urllib.request.Request(API+path, method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())
def recover(cp):
    j=call("POST","/api/recoveries",{"checkpoint":cp})
    jid=j["id"]
    for _ in range(90):
        j=call("GET",f"/api/recoveries/{jid}")
        if j["status"] in ("ready","failed","cancelled"): break
        time.sleep(1)
    assert j["status"]=="ready", j
    return jid
fails=[]
# p1：核定前 → 复测列必须全 NULL
j1=recover("p1")
rows=call("GET",f"/api/recoveries/{j1}/samples")["rows"]
r=rows[0]
ok = r["retest_version"] is None and r["approved_ph"] is None and r["ph_reading_a"] is None and r["ph_reading_b"] is None and float(r["ph"])==7.10
print("p1 (核定前):", {k:r[k] for k in ("ph","retest_version","ph_reading_a","ph_reading_b","approved_ph")})
fails.append(("p1 复测列全空", ok))
# p2：v1 后 v2 前 → 读数 7.12/7.20 与核定 7.16 一同出现，v2 的 7.32 不出现
j2=recover("p2")
rows=call("GET",f"/api/recoveries/{j2}/samples")["rows"]
r=rows[0]
ok = (r["retest_version"]==1 and float(r["ph_reading_a"])==7.12 and float(r["ph_reading_b"])==7.20
      and float(r["approved_ph"])==7.16 and float(r["ph"])==7.10)
print("p2 (v1后v2前):", {k:r[k] for k in ("ph","retest_version","ph_reading_a","ph_reading_b","approved_ph")})
fails.append(("p2 读数与核定7.16同现且无v2", ok))
q=call("POST",f"/api/recoveries/{j2}/query",{"sql":"SELECT version,status,approved_ph FROM ph_retests ORDER BY version"})
versions=[x["version"] for x in q["rows"]]
print("p2 ph_retests 版本:", versions)
fails.append(("p2 只见 v1 登记", versions==[1]))
# 旧核对点仍可恢复查询
j3=recover("p1")
r=call("GET",f"/api/recoveries/{j3}/samples")["rows"][0]
fails.append(("旧核对点 p1 可再查原样", r["approved_ph"] is None and float(r["ph"])==7.10))
# 恢复实例只读兜底
import urllib.error
try:
    call("POST",f"/api/recoveries/{j3}/query",{"sql":"DELETE FROM samples WHERE 1=1"})
    fails.append(("只读拦截写操作", False))
except urllib.error.HTTPError as e:
    fails.append(("只读拦截写操作", e.code==400))
for name, ok in fails: print(("PASS" if ok else "FAIL"), name)
print("RESULT:", "ALL PASS" if all(o for _,o in fails) else "HAS FAILURE")
