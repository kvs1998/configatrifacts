import argparse
import os
import re
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import yaml
from azure.devops.connection import Connection
from azure.devops.v7_1.git.models import (
    GitPullRequestToCreate,
    IdentityRefWithVote,
)
from azure.devops.v7_1.work_item_tracking.models import JsonPatchOperation
from dotenv import load_dotenv
from msrest.authentication import BasicAuthentication

load_dotenv()

PAT = os.environ["AZURE_PAT"]
REPOS_ROOT = os.path.abspath(os.environ.get("REPOS_ROOT", "/tmp/cloned_repos"))
STATE_FILE = ".pr_state.yaml"

# Domain checkboxes present in the template under "Select a Domain"
DOMAIN_OPTIONS = [
    "ABOR",
    "ACT",
    "ALT",
    "ANR",
    "EDP",
    "ESG",
    "IBOR",
    "MKT",
    "REF",
]

TEMPLATE_DESCRIPTION_PLACEHOLDER = (
    "Add a description of what is being changed, and why, "
    "and a Release Packet, if going to production."
)


# ── Logging ───────────────────────────────────────────────────────────────────


class Step:
    def __init__(self):
        self._count = 0

    def reset(self):
        self._count = 0

    def log(self, msg: str):
        self._count += 1
        print(f"      {self._count}. {msg}")

    def warn(self, msg: str):
        self._count += 1
        print(f"      {self._count}. ⚠  {msg}")

    def ok(self, msg: str):
        self._count += 1
        print(f"      {self._count}. ✅ {msg}")

    def error(self, msg: str):
        self._count += 1
        print(f"      {self._count}. ❌ {msg}")


step = Step()


def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print(f"{'═' * 60}")


def subsection(title: str):
    print(f"\n  {'─' * 55}")
    print(f"  {title}")
    print(f"  {'─' * 55}")


def group_header(group: str):
    print(f"\n    ┌─ Group : {group}")


def group_footer(group: str):
    print(f"    └─ Done  : {group}")


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


def load_template(path: str) -> Optional[str]:
    """
    Load template text from path.
    Returns None if path does not exist (with a warning).
    """
    if os.path.exists(path):
        with open(path) as f:
            return f.read()
    print(f"  ⚠  Template file '{path}' not found — no template will be used")
    return None


def resolve_template(
    repo_cfg: dict, global_cfg: dict
) -> Tuple[Optional[str], str]:
    """
    Resolve which template to use for a repo.

    Priority:
      1. repo_cfg["template_file"]  — repo-specific
      2. global_cfg["template_file"] — global fallback
      3. None — no template; description will be auto-content only

    Returns (template_text_or_None, source_label).
    """
    repo_tpl = repo_cfg.get("template_file")
    if repo_tpl:
        text = load_template(repo_tpl)
        if text is not None:
            return text, f"repo-specific ({repo_tpl})"
        # declared but file missing — fall through

    global_tpl = global_cfg.get("template_file")
    if global_tpl:
        text = load_template(global_tpl)
        if text is not None:
            return text, f"global ({global_tpl})"
        # declared but file missing — fall through

    return None, "none (auto-content only)"


# ── Work Item resolution ──────────────────────────────────────────────────────


def collect_work_items(
    files: List[dict],
    group_cfg: Optional[dict],
    repo_cfg: dict,
) -> List[int]:
    """
    Collect all unique work item IDs for a group's PR.

    All three levels are merged and deduplicated:
      - repo-level  : repo_cfg["work_items"]
      - group-level : group_cfg["work_items"]
      - file-level  : each file entry's "work_items"
    """
    ids = set()

    for wid in repo_cfg.get("work_items", []):
        ids.add(int(wid))

    if group_cfg:
        for wid in group_cfg.get("work_items", []):
            ids.add(int(wid))

    for f in files:
        for wid in f.get("work_items", []):
            ids.add(int(wid))

    return sorted(ids)


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Raise PRs in Azure DevOps from staged files.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
examples:
  # run everything
  python raise_pr.py

  # dry run — see plan without making any changes
  python raise_pr.py --dry-run

  # single repo, all groups, all branches
  python raise_pr.py --repo configartifacts

  # single repo, single group, single branch
  python raise_pr.py --repo configartifacts --group IBOR --branch develop

  # multiple repos
  python raise_pr.py --repo configartifacts --repo aladb

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


def filter_repos(
    repos: List[dict], selected: Optional[List[str]]
) -> List[dict]:
    if not selected:
        return repos
    filtered = [r for r in repos if r["name"] in selected]
    missing = set(selected) - {r["name"] for r in filtered}
    if missing:
        print(f"  ⚠  Repos not found in config: {', '.join(missing)}")
    return filtered


def filter_branches(
    branches: List[str], selected: Optional[List[str]]
) -> List[str]:
    if not selected:
        return branches
    filtered = [b for b in branches if b in selected]
    missing = set(selected) - set(filtered)
    if missing:
        print(f"  ⚠  Branches not in repo config: {', '.join(missing)}")
    return filtered


def filter_groups(
    grouped_files: Dict[str, List[dict]], selected: Optional[List[str]]
) -> Dict[str, List[dict]]:
    if not selected:
        return grouped_files
    filtered = {g: f for g, f in grouped_files.items() if g in selected}
    missing = set(selected) - set(filtered)
    if missing:
        print(f"  ⚠  Groups not found after resolution: {', '.join(missing)}")
    return filtered


# ── Group resolution ──────────────────────────────────────────────────────────


def resolve_group(
    dst: str, branch_groups: List[dict], catch_all: str
) -> str:
    for group_cfg in branch_groups:
        if re.search(group_cfg["pattern"], dst):
            return group_cfg["group"]
    return catch_all


def get_group_cfg(
    group: str, branch_groups: List[dict]
) -> Optional[dict]:
    """Return the branch_group config entry for a resolved group name."""
    for g in branch_groups:
        if g["group"] == group:
            return g
    return None


def group_files(
    files: List[dict],
    branch_groups: List[dict],
    catch_all: str,
) -> Dict[str, List[dict]]:
    grouped = defaultdict(list)
    for entry in files:
        group = resolve_group(entry["dst"], branch_groups, catch_all)
        grouped[group].append(entry)
    return dict(grouped)


# ── PR Description ────────────────────────────────────────────────────────────


def build_pr_description(
    template: Optional[str],
    group: str,
    domain_map: Dict[str, str],
    files: List[dict],
    repo_name: str,
    base_branch: str,
    timestamp: str,
) -> str:
    """
    Build the PR description.

    If a template is provided:
      - Auto-check the matching domain checkbox.
      - Inject auto-content at the known placeholder, or append if not found.

    If no template: return auto-content only.
    """
    mapped_domain = domain_map.get(group)

    file_lines = "\n".join(f"  - `{f['dst']}`" for f in files)
    auto_content = (
        f"Automated PR raised by pr-automation script.\n\n"
        f"**Repo:** `{repo_name}`  \n"
        f"**Group:** `{group}`  \n"
        f"**Target branch:** `{base_branch}`  \n"
        f"**Timestamp:** `{timestamp}`  \n\n"
        f"**Files changed:**  \n"
        f"{file_lines}"
    )

    if template is None:
        return auto_content

    description = template

    # Auto-check the matching domain checkbox
    if mapped_domain and mapped_domain in DOMAIN_OPTIONS:
        description = description.replace(
            f"- [ ] {mapped_domain}?",
            f"- [x] {mapped_domain}?",
        )

    # Inject auto-content at the known placeholder
    if TEMPLATE_DESCRIPTION_PLACEHOLDER in description:
        description = description.replace(
            TEMPLATE_DESCRIPTION_PLACEHOLDER,
            auto_content,
        )
    else:
        # Unknown template structure — append at the bottom
        description = description.rstrip() + "\n\n---\n\n" + auto_content

    return description


# ── Git helpers (SSH) ─────────────────────────────────────────────────────────


def get_env() -> dict:
    env = os.environ.copy()
    git_ssh = os.environ.get("GIT_SSH_COMMAND")
    if git_ssh:
        env["GIT_SSH_COMMAND"] = git_ssh
    return env


def run(cmd: List[str], cwd: Optional[str] = None) -> str:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env=get_env(),
    )
    return result.stdout.strip()


def get_ssh_url(ssh_remote_base: str, repo_name: str) -> str:
    return f"{ssh_remote_base}/{repo_name}"


def clone_or_fetch(repo_name: str, ssh_remote_base: str) -> str:
    local_path = os.path.join(REPOS_ROOT, repo_name)
    ssh_url = get_ssh_url(ssh_remote_base, repo_name)

    if not os.path.exists(local_path):
        step.log(f"Cloning '{repo_name}' via SSH into {local_path}")
        run(["git", "clone", ssh_url, local_path])
        step.ok("Clone successful")
    else:
        step.log(f"Repo '{repo_name}' exists locally, fetching all remotes...")
        run(["git", "fetch", "--all"], cwd=local_path)
        step.ok("Fetch successful")

    return local_path


def prepare_branch(local_path: str, base_branch: str, new_branch: str):
    step.log(f"Checking out '{base_branch}'")
    run(["git", "checkout", base_branch], cwd=local_path)

    step.log(f"Pulling latest from origin/{base_branch}")
    run(["git", "pull", "origin", base_branch], cwd=local_path)
    step.ok(f"Up to date with origin/{base_branch}")

    step.log(f"Cutting new branch '{new_branch}' from '{base_branch}'")
    run(["git", "checkout", "-b", new_branch], cwd=local_path)
    step.ok(f"Branch '{new_branch}' ready")


def checkout_existing_branch(
    local_path: str, branch: str, base_branch: str
):
    step.log(f"Switching to '{base_branch}' before fetching feature branch")
    run(["git", "checkout", base_branch], cwd=local_path)

    step.log(f"Fetching existing branch '{branch}' from origin")
    run(["git", "fetch", "origin", branch], cwd=local_path)
    run(["git", "checkout", branch], cwd=local_path)

    step.log(f"Pulling latest commits on '{branch}'")
    run(["git", "pull", "origin", branch], cwd=local_path)
    step.ok(f"Checked out and up to date on '{branch}'")


def copy_files(files: List[dict], repo_local_path: str) -> List[str]:
    copied = []
    for entry in files:
        src = os.path.abspath(entry["src"])
        dst = os.path.join(repo_local_path, entry["dst"])

        if not os.path.exists(src):
            step.warn(f"Source not found, skipping: {entry['src']}")
            continue

        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        step.log(f"Copied : {entry['src']}")
        step.log(f"    └─► {entry['dst']}")
        copied.append(entry["dst"])

    return copied


def has_changes(repo_local_path: str) -> bool:
    status = run(["git", "status", "--porcelain"], cwd=repo_local_path)
    return bool(status.strip())


def commit_and_push(repo_local_path: str, branch: str, message: str):
    step.log("Staging all changes (git add .)")
    run(["git", "add", "."], cwd=repo_local_path)

    step.log(f"Committing with message: '{message}'")
    run(["git", "commit", "-m", message], cwd=repo_local_path)

    step.log(f"Pushing branch '{branch}' to origin")
    run(["git", "push", "origin", branch], cwd=repo_local_path)
    step.ok(f"Push successful → origin/{branch}")


# ── Azure DevOps clients (lazy) ───────────────────────────────────────────────

_clients: Dict[str, object] = {}


def get_git_client(org_url: str):
    if "git" not in _clients:
        step.log("Initializing Azure DevOps connection...")
        credentials = BasicAuthentication("", PAT)
        connection = Connection(base_url=org_url, creds=credentials)
        _clients["git"] = connection.clients.get_git_client()
        _clients["wit"] = connection.clients.get_work_item_tracking_client()
        step.ok("Azure DevOps client ready")
    return _clients["git"]


def get_wit_client():
    """
    Return the work-item-tracking client.
    Must be called after get_git_client() has initialized the connection.
    """
    if "wit" not in _clients:
        raise RuntimeError(
            "WIT client not initialized. Call get_git_client() first."
        )
    return _clients["wit"]


def is_pr_still_open(
    git_client, project: str, repo_name: str, pr_id: int
) -> bool:
    repo = git_client.get_repository(repo_name, project=project)
    pr = git_client.get_pull_request(repo.id, pr_id, project=project)
    return pr.status == "active"


def link_work_items_to_pr(
    git_client,
    wit_client,
    project: str,
    repo_name: str,
    pr_id: int,
    work_item_ids: List[int],
    org_url: str,
):
    """
    Link ADO work items to a PR using the WIT patch API.

    Fetches the PR's server-generated artifact_id so the URL is always
    correct. Skips any IDs that fail (e.g. ticket doesn't exist).
    """
    if not work_item_ids:
        return

    repo = git_client.get_repository(repo_name, project=project)

    # Use the server-generated artifact_id — never construct it manually
    pr = git_client.get_pull_request(
        repository_id=repo.id, pull_request_id=pr_id, project=project
    )
    pr_artifact_id = pr.artifact_id

    for wid in work_item_ids:
        try:
            patch = [
                JsonPatchOperation(
                    op="add",
                    path="/relations/-",
                    value={
                        "rel": "ArtifactLink",
                        "url": pr_artifact_id,
                        "attributes": {"name": "Pull Request"},
                    },
                )
            ]
            # Note: omit project= for cross-project work item support
            wit_client.update_work_item(document=patch, id=wid)
            step.ok(f"Linked work item #{wid} → PR #{pr_id}")
        except Exception as e:
            step.warn(f"Could not link work item #{wid}: {e}")


def raise_pr(
    git_client,
    project: str,
    repo_name: str,
    source_branch: str,
    target_branch: str,
    title: str,
    org_url: str,
    reviewers: List[str],
    description: str,
) -> int:
    repo = git_client.get_repository(repo_name, project=project)

    reviewer_refs = (
        [IdentityRefWithVote(id=guid) for guid in reviewers]
        if reviewers
        else []
    )

    pr_payload = GitPullRequestToCreate(
        title=title,
        description=description,
        source_ref_name=f"refs/heads/{source_branch}",
        target_ref_name=f"refs/heads/{target_branch}",
        reviewers=reviewer_refs,
    )

    created = git_client.create_pull_request(
        pr_payload, repo.id, project=project
    )
    pr_url = (
        f"{org_url}/{project}/_git/{repo_name}"
        f"/pullrequest/{created.pull_request_id}"
    )
    step.ok(f"PR #{created.pull_request_id} raised successfully")
    step.log(f"URL → {pr_url}")
    if reviewers:
        step.log(f"Reviewers assigned : {len(reviewers)}")

    return created.pull_request_id


# ── Dry run ───────────────────────────────────────────────────────────────────


def print_dry_run_plan(
    repo_name: str,
    grouped_files: Dict[str, List[dict]],
    target_branches: List[str],
    state: dict,
    reviewers: List[str],
    domain_map: Dict[str, str],
    template_source: str,
    branch_groups: List[dict],
    repo_cfg: dict,
):
    print(f"\n  Template source : {template_source}")

    for base_branch in target_branches:
        subsection(f"DRY RUN | {repo_name} → {base_branch}")
        i = 0
        for group, files in grouped_files.items():
            state_entry = (
                state.get(repo_name, {})
                .get(group, {})
                .get(base_branch, {})
            )
            existing_pr = state_entry.get("pr_id")
            existing_br = state_entry.get("branch")
            mapped_domain = domain_map.get(group, "none")
            group_cfg = get_group_cfg(group, branch_groups)
            work_items = collect_work_items(files, group_cfg, repo_cfg)

            group_header(group)

            i += 1
            if existing_pr:
                print(f"      {i}. ACTION    : AMEND existing PR")
                print(f"         Branch    : {existing_br}")
                print(f"         PR        : #{existing_pr}")
            else:
                print(f"      {i}. ACTION    : NEW branch + PR")
                print(
                    f"         Branch    : "
                    f"auto/{group}-{base_branch}-<timestamp>"
                )

            i += 1
            print(f"      {i}. Domain    : {mapped_domain} (will be checked)")

            i += 1
            print(f"      {i}. Files     :")
            for f in files:
                wi = f.get("work_items", [])
                wi_str = f"  [tickets: {wi}]" if wi else ""
                print(f"           {f['src']}{wi_str}")
                print(f"           └─► {f['dst']}")

            i += 1
            if work_items:
                print(
                    f"      {i}. Work items to link"
                    f" ({len(work_items)}): {work_items}"
                )
            else:
                print(f"      {i}. Work items : none")

            if reviewers:
                i += 1
                print(f"      {i}. Reviewers ({len(reviewers)}):")
                for r in reviewers:
                    print(f"           • {r}")

            group_footer(group)


# ── Per group logic ───────────────────────────────────────────────────────────


def process_group(
    repo_name: str,
    group: str,
    files: List[dict],
    base_branch: str,
    timestamp: str,
    local_path: str,
    state: dict,
    git_client,
    global_cfg: dict,
    reviewers: List[str],
    domain_map: Dict[str, str],
    template: Optional[str],
    branch_groups: List[dict],
    repo_cfg: dict,
):
    org_url = global_cfg["azure"]["org_url"]
    project = global_cfg["azure"]["project"]

    state_entry = (
        state.setdefault(repo_name, {})
        .setdefault(group, {})
        .get(base_branch, {})
    )
    existing_branch = state_entry.get("branch")
    existing_pr_id = state_entry.get("pr_id")
    is_amendment = False

    group_cfg = get_group_cfg(group, branch_groups)
    work_items = collect_work_items(files, group_cfg, repo_cfg)

    # ── Step 1: determine branch strategy ────────────────────────────────────
    step.log("Checking for existing open PR in state...")
    if existing_branch and existing_pr_id:
        if is_pr_still_open(git_client, project, repo_name, existing_pr_id):
            step.log(
                f"Open PR #{existing_pr_id} found on '{existing_branch}'"
                f" — will amend"
            )
            checkout_existing_branch(local_path, existing_branch, base_branch)
            is_amendment = True
        else:
            step.log(
                f"PR #{existing_pr_id} is closed/merged"
                f" — will create fresh branch"
            )
            existing_branch = None
            existing_pr_id = None
    else:
        step.log("No existing PR found — will create new branch + PR")

    if not is_amendment:
        new_branch = f"auto/{group}-{base_branch}-{timestamp}"
        prepare_branch(local_path, base_branch, new_branch)
    else:
        new_branch = existing_branch

    # ── Step 2: copy files ────────────────────────────────────────────────────
    step.log(f"Copying {len(files)} file(s) into repo...")
    copy_files(files, local_path)

    # ── Step 3: check diff ────────────────────────────────────────────────────
    step.log("Checking git diff...")
    if not has_changes(local_path):
        step.warn(
            f"No changes detected after copy — skipping PR for"
            f" group '{group}' → '{base_branch}'"
        )
        run(["git", "checkout", base_branch], cwd=local_path)
        if not is_amendment:
            run(["git", "branch", "-D", new_branch], cwd=local_path)
        return
    step.ok("Changes detected — proceeding")

    # ── Step 4: commit + push ─────────────────────────────────────────────────
    commit_msg = (
        f"chore({group}): {'amend' if is_amendment else 'initial'}"
        f" changes → {base_branch} [{timestamp}]"
    )
    commit_and_push(local_path, new_branch, commit_msg)

    # ── Step 5: raise PR or log amendment + link work items ──────────────────
    if not is_amendment:
        step.log("Building PR description...")
        description = build_pr_description(
            template,
            group,
            domain_map,
            files,
            repo_name,
            base_branch,
            timestamp,
        )
        mapped_domain = domain_map.get(group, "none")
        step.log(f"Domain checkbox auto-checked : [{mapped_domain}]")

        step.log("Raising PR in Azure DevOps...")
        pr_title = (
            f"[AUTO] {repo_name}/{group} → {base_branch} | {timestamp}"
        )
        pr_id = raise_pr(
            git_client,
            project,
            repo_name,
            new_branch,
            base_branch,
            pr_title,
            org_url,
            reviewers,
            description,
        )
        state[repo_name][group][base_branch] = {
            "branch": new_branch,
            "pr_id": pr_id,
        }

        if work_items:
            step.log(
                f"Linking {len(work_items)} work item(s)"
                f" to PR #{pr_id}: {work_items}"
            )
            link_work_items_to_pr(
                git_client,
                get_wit_client(),
                project,
                repo_name,
                pr_id,
                work_items,
                org_url,
            )
        else:
            step.log("No work items configured for this group — skipping link")

    else:
        step.ok(
            f"Amendment pushed — PR #{existing_pr_id} updated automatically"
        )
        if work_items:
            step.log(
                f"Linking {len(work_items)} work item(s)"
                f" to existing PR #{existing_pr_id}: {work_items}"
            )
            link_work_items_to_pr(
                git_client,
                get_wit_client(),
                project,
                repo_name,
                existing_pr_id,
                work_items,
                org_url,
            )
        else:
            step.log("No work items configured for this group — skipping link")

    run(["git", "checkout", base_branch], cwd=local_path)


# ── Per repo logic ────────────────────────────────────────────────────────────


def process_repo(
    repo_cfg: dict,
    timestamp: str,
    global_cfg: dict,
    state: dict,
    args: argparse.Namespace,
):
    repo_name = repo_cfg["name"]
    files = repo_cfg.get("files", [])
    branch_groups = repo_cfg.get("branch_groups", [])
    catch_all = global_cfg["defaults"]["catch_all_branch_prefix"]
    ssh_remote_base = global_cfg["git"]["ssh_remote_base"]
    reviewers = repo_cfg.get("reviewers", [])
    domain_map = repo_cfg.get("domain_map", {})

    target_branches = filter_branches(
        repo_cfg.get("target_branches", ["develop"]), args.branches
    )

    section(f"Repo : {repo_name}")

    # Resolve template for this repo
    template, template_source = resolve_template(repo_cfg, global_cfg)
    step.log(f"PR template : {template_source}")

    if not files:
        print("  ⚠  No files configured, skipping.")
        return

    if not target_branches:
        print("  ⚠  No matching target branches, skipping.")
        return

    # ── Resolve + print group plan ────────────────────────────────────────────
    print("\n  Resolving file → group mappings...")
    grouped_files = group_files(files, branch_groups, catch_all)
    for grp, grp_files in grouped_files.items():
        print(f"    [{grp}] — {len(grp_files)} file(s)")
        for f in grp_files:
            wi = f.get("work_items", [])
            wi_str = f"  [tickets: {wi}]" if wi else ""
            print(f"         {f['src']}{wi_str}")
            print(f"         └─► {f['dst']}")

    grouped_files = filter_groups(grouped_files, args.groups)

    if not grouped_files:
        print("  ⚠  No matching groups, skipping.")
        return

    if args.dry_run:
        print_dry_run_plan(
            repo_name,
            grouped_files,
            target_branches,
            state,
            reviewers,
            domain_map,
            template_source,
            branch_groups,
            repo_cfg,
        )
        return

    git_client = get_git_client(global_cfg["azure"]["org_url"])

    print("\n  Initializing local repo...")
    step.reset()
    local_path = clone_or_fetch(repo_name, ssh_remote_base)

    for base_branch in target_branches:
        subsection(f"{repo_name} → {base_branch}")
        for group, group_file_list in grouped_files.items():
            group_header(group)
            step.reset()
            process_group(
                repo_name,
                group,
                group_file_list,
                base_branch,
                timestamp,
                local_path,
                state,
                git_client,
                global_cfg,
                reviewers,
                domain_map,
                template,
                branch_groups,
                repo_cfg,
            )
            group_footer(group)


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    args = parse_args()
    config = load_config()
    state = load_state()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    print("\n" + "═" * 60)
    print("  PR Automation — Azure DevOps")
    print(f"  Run timestamp : {timestamp}")
    if args.dry_run:
        print("  Mode          : DRY RUN (no changes will be made)")
    print("═" * 60)

    repos = filter_repos(config["repos"], args.repos)
    print(f"\n  Repos to process : {len(repos)}")
    for r in repos:
        print(f"    • {r['name']}")

    for repo_cfg in repos:
        try:
            process_repo(repo_cfg, timestamp, config, state, args)
        except subprocess.CalledProcessError as e:
            print(f"\n  ❌ Git error for {repo_cfg['name']}: {e.stderr}")
        except Exception as e:
            print(f"\n  ❌ Error for {repo_cfg['name']}: {e}")

    if not args.dry_run:
        save_state(state)

    print("\n" + "═" * 60)
    print("  All done.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
