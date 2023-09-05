# for tests only
def _patch_repodata(repodata, subdir):
    repodata["packages"]
    instructions = {
        "patch_instructions_version": 1,
        "packages": {},
        "revoke": [],
        "remove": [],
    }
    return instructions
