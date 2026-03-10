import os
from git import Repo

def get_current_branch():
    try:
        repo = Repo(search_parent_directories=True)
        return repo.active_branch.name
    except:
        return "unknown"

def get_git_commit_hash():
    try:
        repo = Repo(search_parent_directories=True)
        return repo.head.commit.hexsha[:6]
    except:
        raise ValueError("Cannot get git commit hash")

def get_schema(base_prefix):
    if os.getenv("SCHEMA"):
        return os.getenv("SCHEMA")
    current_branch = get_current_branch()
    if current_branch == "main":
        current_branch = get_git_commit_hash()
    safe_branch_name = current_branch.replace("/", "_").replace("-", "_").replace(".", "_").replace(" ", "_").lower()
    return f"{base_prefix}_{safe_branch_name}"
