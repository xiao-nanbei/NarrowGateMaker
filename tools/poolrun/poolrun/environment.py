"""Explicit dependency-file approval at its final path. Never relocates or pip-upgrades a live env."""
from pathlib import Path
from .release import relative,sha
from .store import digest


def seal(root,python,files):
    root=Path(root).resolve();python=Path(python).absolute()
    inventory={}
    for name in files:
        name=relative(name.strip());p=root/name
        if p.is_symlink() or root not in p.resolve().parents or not p.is_file():raise ValueError("environment inventory must name in-root regular files")
        inventory[name]={"sha256":sha(p),"size":p.stat().st_size}
    result={"python":str(python),"python_sha256":sha(python.resolve()),"root":str(root),"files":inventory}
    result["manifest_sha256"]=digest(inventory)
    return result


def verify(definition):
    exe=Path(definition["python"]).resolve()
    if not exe.is_file() or sha(exe)!=definition["python_sha256"]:raise ValueError("unapproved interpreter")
    files=definition.get("files",{})
    if files:
        if digest(files)!=definition["manifest_sha256"]:raise ValueError("environment inventory changed")
        root=Path(definition["root"]).resolve()
        for name,metadata in files.items():
            p=root/relative(name)
            if p.is_symlink() or root not in p.resolve().parents or p.stat().st_size!=metadata["size"] or sha(p)!=metadata["sha256"]:
                raise ValueError("environment dependency differs: "+name)
    return True
