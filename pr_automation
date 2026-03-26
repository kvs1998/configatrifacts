import argparse
import os
import re
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime

import yaml
from azure.devops.connection import Connection
from azure.devops.v7_0.git.models import GitPullRequestToCreate
from dotenv import load_dotenv
from msrest.authentication import BasicAuthentication

load_dotenv()

PAT        = os.environ["AZURE_PAT"]
REPOS_ROOT = os.path.abspath(os.environ.get("REPOS_ROOT", "/tmp/cloned_repos"))
STATE_FILE = ".pr_state.yaml"


# ── Config + State ────────────────────────────────────────────────────────────

def load_config(path: str = "config/repos.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        yaml.dump(state, f, default_flow_style=False)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Raise PRs in Azure DevOps from staged files.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
examples:
  # run everything
  python raise_pr.py

  # single repo, all groups, all branches
  python raise_pr.py --repo configartifacts

  # single repo, single group, all branches
  python raise_pr.py --repo configartifacts --group IBOR

  # single repo, single group, single branch
  python raise_pr.py --repo configartifacts --group IBOR --branch develop

  # single repo, all groups, single branch
  python raise_pr.py --repo configartifacts --branch main

  # multiple repos
  python raise_pr.py --repo configartifacts --repo aladdindb

  # multiple groups
  python raise_pr.py --repo configartifacts --group IBOR --group REF
        """,
    )

    parser.add_argument(
        "--repo",
        dest="repos",
        action="append",
        metavar="REPO_NAME",
        help="Repo(s) to process. Repeatable. Omit for all repos.",
    )
    parser.add_argument(
        "--group",
        dest="groups",
        action="append",
        metavar="GROUP_NAME",
        help="Group(s) to process. Repeatable. Omit for all groups.",
    )
    parser.add_argument(
        "--branch",
        dest="branches",
        action="append",
        metavar="BRANCH_NAME",
        help="Target branch(es) to process. Repeatable. Omit for all branches.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve groups and print plan without making any changes.",
    )

    return parser.parse_args()


def filter_repos(repos: list[dict], selected: list[str] | None) -> list[dict]:
    if not selected:
        return repos
    filtered = [r for r in repos if r["name"] in selected]
    missing  = set(selected) - {r["name"] for r in filtered}
    if missing:
        print(f"  ⚠  Repos not found in config: {', '.join(missing)}")
    return filtered


def filter_branches(
    branches: list[str], selected: list[str] | None
) -> list[str]:
    if not selected:
        return branches
    filtered = [b for b in branches if b in selected]
    missing  = set(selected) - set(filtered)
    if missing:
        print(f"  ⚠  Branches not found in repo config: {', '.join(missing)}")
    return filtered


def filter_groups(
    grouped_files: dict[str, list[dict]], selected: list[str] | None
) -> dict[str, list[dict]]:
    if not selected:
        return grouped_files
    filtered = {g: f for g, f in grouped_files.items() if g in selected}
    missing  = set(selected) - set(filtered)
    if missing:
        print(f"  ⚠  Groups not found after resolution: {', '.join(missing)}")
    return filtered


# ── Group resolution ──────────────────────────────────────────────────────────

def resolve_group(dst: str, branch_groups: list[dict], catch_all: str) -> str:
    for group_cfg in branch_groups:
        if re.search(group_cfg["pattern"], dst):
            return group_cfg["group"]
    return catch_all


def group_files(
    files: list[dict],
    branch_groups: list[dict],
    catch_all: str,
) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for entry in files:
        group = resolve_group(entry["dst"], branch_groups, catch_all)
        grouped[group].append(entry)
        print(f"    [{group}] ← {entry['dst']}")
    return dict(grouped)


# ── Git helpers (SSH) ─────────────────────────────────────────────────────────

def run(cmd: list[str], cwd: str = None) -> str:
    result = subprocess.run(
        cmd, cwd=cwd, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def get_ssh_url(ssh_remote_base: str, repo_name: str) -> str:
    return f"{ssh_remote_base}/{repo_name}"


def clone_or_fetch(repo_name: str, ssh_remote_base: str) -> str:
    local_path = os.path.join(REPOS_ROOT, repo_name)
    ssh_url    = get_ssh_url(ssh_remote_base, repo_name)

    if not os.path.exists(local_path):
        print(f"    Cloning {repo_name} via SSH...")
        run(["git", "clone", ssh_url, local_path])
    else:
        print(f"    Fetching latest for {repo_name}...")
        run(["git", "fetch", "--all"], cwd=local_path)

    return local_path


def prepare_branch(local_path: str, base_branch: str, new_branch: str):
    print(f"    Checking out '{base_branch}' and pulling latest...")
    run(["git", "checkout", base_branch], cwd=local_path)
    run(["git", "pull", "origin", base_branch], cwd=local_path)
    print(f"    Cutting branch: {new_branch}")
    run(["git", "checkout", "-b", new_branch], cwd=local_path)


def checkout_existing_branch(
    local_path: str, branch: str, base_branch: str
):
    print(f"    Checking out existing branch: {branch}")
    run(["git", "checkout", base_branch], cwd=local_path)
    run(["git", "fetch", "origin", branch], cwd=local_path)
    run(["git", "checkout", branch], cwd=local_path)
    run(["git", "pull", "origin", branch], cwd=local_path)


def copy_files(files: list[dict], repo_local_path: str) -> list[str]:
    copied = []
    for entry in files:
        src = os.path.abspath(entry["src"])
        dst = os.path.join(repo_local_path, entry["dst"])

        if not os.path.exists(src):
            print(f"    ⚠  Source not found, skipping: {entry['src']}")
            continue

        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        print(f"    Copied: {entry['src']}  →  {entry['dst']}")
        copied.append(entry["dst"])

    return copied


def has_changes(repo_local_path: str) -> bool:
    status = run(["git", "status", "--porcelain"], cwd=repo_local_path)
    return bool(status.strip())


def commit_and_push(repo_local_path: str, branch: str, message: str):
    run(["git", "add", "."], cwd=repo_local_path)
    run(["git", "commit", "-m", message], cwd=repo_local_path)
    run(["git", "push", "origin", branch], cwd=repo_local_path)
    print(f"    Pushed to branch: {branch}")


# ── PR helpers ────────────────────────────────────────────────────────────────

def is_pr_still_open(
    git_client, project: str, repo_name: str, pr_id: int
) -> bool:
    repo = git_client.get_repository(repo_name, project=project)
    pr   = git_client.get_pull_request(repo.id, pr_id, project=project)
    return pr.status == "active"


def raise_pr(
    git_client,
    project: str,
    repo_name: str,
    source_branch: str,
    target_branch: str,
    title: str,
    org_url: str,
) -> int:
    repo = git_client.get_repository(repo_name, project=project)

    pr_payload = GitPullRequestToCreate(
        title=title,
        description="Automated PR — please review and merge manually.",
        source_ref_name=f"refs/heads/{source_branch}",
        target_ref_name=f"refs/heads/{target_branch}",
    )

    created = git_client.create_pull_request(
        pr_payload, repo.id, project=project
    )
    pr_url = (
        f"{org_url}/{project}/_git/{repo_name}"
        f"/pullrequest/{created.pull_request_id}"
    )
    print(f"    ✅ PR #{created.pull_request_id} raised → {pr_url}")
    return created.pull_request_id


# ── Dry run ───────────────────────────────────────────────────────────────────

def print_dry_run_plan(
    repo_name: str,
    grouped_files: dict[str, list[dict]],
    target_branches: list[str],
    state: dict,
):
    print(f"\n  {'─' * 50}")
    print(f"  DRY RUN — Repo: {repo_name}")
    for base_branch in target_branches:
        print(f"\n    Target branch: {base_branch}")
        for group, files in grouped_files.items():
            state_entry = (
                state
                .get(repo_name, {})
                .get(group, {})
                .get(base_branch, {})
            )
            existing_pr = state_entry.get("pr_id")
            existing_br = state_entry.get("branch")

            if existing_pr:
                action = f"AMEND   → push to existing branch '{existing_br}' (PR #{existing_pr})"
            else:
                action = f"NEW PR  → cut branch 'auto/{group}-{base_branch}-<timestamp>'"

            print(f"      [{group}] {action}")
            for f in files:
                print(f"        {f['src']}  →  {f['dst']}")


# ── Per group logic ───────────────────────────────────────────────────────────

def process_group(
    repo_name: str,
    group: str,
    files: list[dict],
    base_branch: str,
    timestamp: str,
    local_path: str,
    state: dict,
    git_client,
    global_cfg: dict,
):
    org_url = global_cfg["azure"]["org_url"]
    project = global_cfg["azure"]["project"]

    state_entry    = (
        state
        .setdefault(repo_name, {})
        .setdefault(group, {})
        .get(base_branch, {})
    )
    existing_branch = state_entry.get("branch")
    existing_pr_id  = state_entry.get("pr_id")
    is_amendment    = False

    if existing_branch and existing_pr_id:
        if is_pr_still_open(git_client, project, repo_name, existing_pr_id):
            print(
                f"    Open PR #{existing_pr_id} found, "
                f"amending existing branch..."
            )
            checkout_existing_branch(local_path, existing_branch, base_branch)
            is_amendment = True
        else:
            print(
                f"    PR #{existing_pr_id} is closed/merged, "
                f"creating fresh branch..."
            )
            existing_branch = None
            existing_pr_id  = None

    if not is_amendment:
        new_branch = f"auto/{group}-{base_branch}-{timestamp}"
        prepare_branch(local_path, base_branch, new_branch)
    else:
        new_branch = existing_branch

    copy_files(files, local_path)

    if not has_changes(local_path):
        print(
            f"    ⚠  No changes detected for "
            f"group '{group}' → '{base_branch}', skipping."
        )
        run(["git", "checkout", base_branch], cwd=local_path)
        if not is_amendment:
            run(["git", "branch", "-D", new_branch], cwd=local_path)
        return

    commit_msg = (
        f"chore({group}): {'amend' if is_amendment else 'initial'} "
        f"changes → {base_branch} [{timestamp}]"
    )
    commit_and_push(local_path, new_branch, commit_msg)

    if not is_amendment:
        pr_title = (
            f"[AUTO] {repo_name}/{group} → {base_branch} | {timestamp}"
        )
        pr_id = raise_pr(
            git_client, project, repo_name,
            new_branch, base_branch, pr_title, org_url,
        )
        state[repo_name][group][base_branch] = {
            "branch": new_branch,
            "pr_id":  pr_id,
        }
    else:
        print(f"    ✅ Amendment pushed to PR #{existing_pr_id}")

    run(["git", "checkout", base_branch], cwd=local_path)


# ── Per repo logic ────────────────────────────────────────────────────────────

def process_repo(
    repo_cfg: dict,
    timestamp: str,
    global_cfg: dict,
    git_client,
    state: dict,
    args: argparse.Namespace,
):
    repo_name     = repo_cfg["name"]
    files         = repo_cfg.get("files", [])
    branch_groups = repo_cfg.get("branch_groups", [])
    catch_all     = global_cfg["defaults"]["catch_all_branch_prefix"]
    ssh_remote_base = global_cfg["git"]["ssh_remote_base"]

    target_branches = filter_branches(
        repo_cfg.get("target_branches", ["develop"]), args.branches
    )

    print(f"\n{'─' * 55}")
    print(f"  Repo : {repo_name}")

    if not files:
        print(f"  ⚠  No files configured, skipping.")
        return

    if not target_branches:
        print(f"  ⚠  No matching target branches, skipping.")
        return

    print(f"\n  Resolving file groups...")
    grouped_files = group_files(files, branch_groups, catch_all)
    grouped_files = filter_groups(grouped_files, args.groups)

    if not grouped_files:
        print(f"  ⚠  No matching groups, skipping.")
        return

    if args.dry_run:
        print_dry_run_plan(repo_name, grouped_files, target_branches, state)
        return

    local_path = clone_or_fetch(repo_name, ssh_remote_base)

    for base_branch in target_branches:
        print(f"\n  Target branch : {base_branch}")
        for group, group_file_list in grouped_files.items():
            print(f"\n    Group : {group}")
            process_group(
                repo_name, group, group_file_list,
                base_branch, timestamp, local_path,
                state, git_client, global_cfg,
            )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args      = parse_args()
    config    = load_config()
    state     = load_state()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    credentials = BasicAuthentication("", PAT)
    connection  = Connection(
        base_url=config["azure"]["org_url"], creds=credentials
    )
    git_client = connection.clients.get_git_client()

    repos = filter_repos(config["repos"], args.repos)

    for repo_cfg in repos:
        try:
            process_repo(
                repo_cfg, timestamp, config,
                git_client, state, args,
            )
        except subprocess.CalledProcessError as e:
            print(f"\n  ❌ Git error for {repo_cfg['name']}: {e.stderr}")
        except Exception as e:
            print(f"\n  ❌ Error for {repo_cfg['name']}: {e}")

    if not args.dry_run:
        save_state(state)

    print(f"\n{'─' * 55}")
    print("  Done.")


if __name__ == "__main__":
    main()
