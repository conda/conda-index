def _patch_repodata(repodata, subdir):
    patched = {}
    for filename in repodata.get("packages", {}):
        if filename.startswith("conda-index-pkg-a"):
            patched[filename] = {"depends": ["patched-dep"]}
    return {
        "patch_instructions_version": 1,
        "packages": patched,
        "revoke": [],
        "remove": [],
    }
