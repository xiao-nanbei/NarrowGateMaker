"""Conservative locality planning; unknown links receive a nonzero cost."""
from . import model


def preparation_host(state, task, reports):
    candidates=[]
    key=model.release_key(task["spec"]["project"],task["code_policy"]["release"])
    for host,report in reports.items():
        if state["hosts"][host].get("draining"):continue
        synthetic=dict(report,releases=list(set(report.get("releases",[]))|{key}),objects=task["spec"].get("inputs",[]),prepared=[__import__("poolrun.store",fromlist=["digest"]).digest(task["spec"].get("prepared"))])
        reason=model.eligible(state,task,host,synthetic,__import__("time").time())
        if reason:continue
        missing=set(task["spec"].get("inputs",[]))-set(report.get("objects",[]))
        seconds=0; samples=0
        for oid in missing:
            obj=state["objects"][oid]
            options=[]
            for source in obj.get("sources",[]):
                link=source.get("source_id",source.get("host","shared"))+"->"+host
                estimate=state.get("transfer_estimates",{}).get(link)
                if estimate and estimate["seconds"]>0:
                    options.append((obj["size"]/(estimate["bytes"]/estimate["seconds"]),estimate["samples"]))
            cost,count=min(options) if options else (obj["size"]/(1024*1024)+30,0)
            seconds+=cost;samples+=count
        # Running/uncertain work reserves resources; never count a lost host idle.
        candidates.append((seconds,model.resources_used(state,host)["slots"],host,samples))
    if not candidates:return None
    cost,_,host,samples=min(candidates)
    return {"host":host,"estimated_transfer_seconds":cost,"samples":samples,"unknown_link_default_mib_s":1}
