# Publish to GitHub

This package is ready to go into a Git repository. Two sane paths are below.

## Option A: Create an empty repo on GitHub, then push with Git

1. On GitHub, create a **new empty repository**.
2. Do **not** initialize it with a README, license, or `.gitignore`.
3. In this project directory, run:

```bash
git init -b main
git add .
git commit -m "Initial commit: CalR Guard 0.3.1"
git remote add origin https://github.com/YOUR-USER-OR-ORG/calr-guard.git
git push -u origin main
```

## Option B: Use GitHub CLI

If you already use GitHub CLI:

```bash
git init -b main
git add .
git commit -m "Initial commit: CalR Guard 0.3.1"
gh repo create YOUR-USER-OR-ORG/calr-guard --private --source=. --push
```

## Recommended first push settings

- Start with a **private** repository.
- Add the professor or lab as collaborator after the first push.
- Make it public only after maintainer name, citation metadata, and license choice are confirmed.


## Shortcut scripts

Linux / macOS:

```bash
./scripts/push_to_github.sh https://github.com/YOUR-USER-OR-ORG/calr-guard.git
```

Windows:

```bat
scripts\push_to_github.bat https://github.com/YOUR-USER-OR-ORG/calr-guard.git
```
