# for tests only
def _patch_repodata(repodata, subdir):
    repodata["packages"]
    instructions = {
        "patch_instructions_version": 2,
        "packages": {},
        "revoke": [],
        "remove": [],
    }
    return instructions
