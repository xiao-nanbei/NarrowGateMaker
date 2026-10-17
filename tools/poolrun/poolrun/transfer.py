"""Explicit single-hop routes and durable transfer reservations.

Reservations on an unreachable host are NOT silently freed on lease expiry. This
trades availability for obeying bandwidth quotas until operator reconciliation.
"""
import time
import uuid


def admit(s, host, obj, source, policy):
    active = [x for x in s.setdefault("transfers", {}).values() if x["status"] == "ACTIVE"]
    for x in active:
        if x["host"] == host and x["object_id"] == obj["id"]: return x
    route = source["route"]
    if route not in policy.get("routes", {}): raise ValueError("route is not approved by master")
    rules = policy["routes"][route]
    src_zone = source.get("zone", "noncloud")
    dst_zone = s["hosts"][host].get("network_zone", "noncloud")
    if route != "netdisk" and obj["size"] > policy.get("small_file_bytes", 1024*1024) and (src_zone != "cloud" or dst_zone != "cloud"):
        raise ValueError("large noncloud traffic must use netdisk")
    if rules.get("pairs") and [src_zone,dst_zone] not in rules["pairs"]: raise ValueError("zone pair denied")
    source_id = source.get("source_id", source.get("host", "shared"))
    account = source.get("account", "default")
    link = source_id + "->" + host
    checks = [(len(active), policy.get("global_concurrency",3)),
        (sum(x["host"]==host for x in active),1),
        (sum(x["source_id"]==source_id for x in active),rules.get("source_concurrency",3)),
        (sum(x["account"]==account for x in active),rules.get("account_concurrency",3)),
        (sum(x["link"]==link for x in active),rules.get("link_concurrency",1))]
    if any(n>=limit for n,limit in checks): raise ValueError("WAIT_TRANSFER: quota reserved")
    record = {"id":uuid.uuid4().hex,"host":host,"object_id":obj["id"],"source_id":source_id,"account":account,
        "link":link,"route":route,"status":"ACTIVE","created":time.time(),"size":obj["size"],
        "rate_bytes_per_second":rules.get("rate_bytes_per_second",0)}
    s["transfers"][record["id"]] = record
    return record


def finish(s, host, transfer_id, success, elapsed):
    r=s["transfers"][transfer_id]
    if r["host"]!=host: raise ValueError("wrong transfer owner")
    r.update(status="COMPLETE" if success else "FAILED",elapsed=max(0,elapsed))
    if success and elapsed>0:
        key=r["link"]
        estimate=s.setdefault("transfer_estimates",{}).setdefault(key,{"samples":0,"bytes":0,"seconds":0})
        estimate["samples"]+=1;estimate["bytes"]+=r["size"];estimate["seconds"]+=elapsed
    return {"released":transfer_id}
