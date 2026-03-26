# run everything
python scripts/raise_pr.py

# dry run — see plan without making any changes
python scripts/raise_pr.py --dry-run

# single repo
python scripts/raise_pr.py --repo configartifacts

# single repo + single group
python scripts/raise_pr.py --repo configartifacts --group IBOR

# single repo + single group + single branch
python scripts/raise_pr.py --repo configartifacts --group IBOR --branch develop

# multiple repos
python scripts/raise_pr.py --repo configartifacts --repo aladdindb

# multiple groups
python scripts/raise_pr.py --repo configartifacts --group IBOR --group REF

# dry run scoped to one group
python scripts/raise_pr.py --repo configartifacts --group IBOR --dry-run
