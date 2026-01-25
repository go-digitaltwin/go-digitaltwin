# GitHub Advanced Security Configuration Guide

This repository has been configured with GitHub Advanced Security features. Some features are enabled via workflow files (already committed), while others require manual configuration in the GitHub repository settings.

## ✅ Automated via Workflows (Already Configured)

The following security features are enabled through GitHub Actions workflows:

### 1. CodeQL Analysis (`.github/workflows/codeql.yml`)
- **What it does**: Static code analysis to identify security vulnerabilities and coding errors
- **Runs on**: Push/PR to main, weekly schedule (Monday 6am UTC)
- **Results**: Code Scanning alerts in the Security tab

### 2. Go Vulnerability Scanning (`.github/workflows/govulncheck.yml`)
- **What it does**: Scans for known vulnerabilities in Go code, dependencies, and standard library
- **Runs on**: Push/PR to main, weekly schedule (Monday 6am UTC), manual dispatch
- **Results**: Code Scanning alerts in the Security tab (SARIF format)
- **Note**: Uses runtime analysis to only flag vulnerabilities in actually-called code paths

### 3. Dependabot (`.github/dependabot.yml`)
- **What it does**: Monitors Go modules and GitHub Actions for security updates
- **Runs**: Weekly checks on Monday at 6am UTC
- **Results**: Auto-generated PRs for dependency security updates

## ⚙️ Manual Configuration Required

The following features must be enabled manually in the GitHub repository settings:

### 1. Secret Scanning Alerts
**Location**: `Settings` → `Security` → `Code security and analysis`

1. Navigate to your repository settings
2. Click on "Security" in the left sidebar
3. Under "Code security and analysis", find "Secret scanning"
4. Click "Enable" for Secret scanning

**What it does**: Detects secrets (API keys, tokens, credentials) committed to the repository

### 2. Push Protection
**Location**: `Settings` → `Security` → `Code security and analysis`

1. In the same "Code security and analysis" section
2. Find "Push protection"
3. Click "Enable" for Push protection

**What it does**: Blocks commits that contain known secret patterns from being pushed

### 3. Dependabot Alerts
**Location**: `Settings` → `Security` → `Code security and analysis`

1. In the same "Code security and analysis" section
2. Find "Dependabot alerts"
3. Click "Enable" for Dependabot alerts

**What it does**: Notifies about known vulnerabilities in dependencies

### 4. Dependabot Security Updates
**Location**: `Settings` → `Security` → `Code security and analysis`

1. In the same "Code security and analysis" section
2. Find "Dependabot security updates"
3. Click "Enable" for Dependabot security updates

**What it does**: Automatically creates pull requests to update vulnerable dependencies

## 🔒 Branch Protection (Recommended)

To enforce that vulnerabilities must be fixed before merging, configure branch protection rules:

**Location**: `Settings` → `Branches` → `Branch protection rules` → `main`

Add the following required status checks:
- `CodeQL Analysis / Analyze`
- `Go Vulnerability Scan / Run govulncheck`

**Why**: The govulncheck workflow uploads findings but does not fail the job. Without branch protection, PRs with vulnerabilities can still be merged.

## 📊 Monitoring Security

After configuration, monitor security in the following locations:

1. **Security Tab**: View all Code Scanning alerts, Secret Scanning alerts, and Dependabot alerts
2. **Pull Requests**: See inline annotations on PR diffs for vulnerabilities detected in changed files
3. **Actions Tab**: Monitor workflow runs for CodeQL and govulncheck

## 🔄 Workflow Triggers

All security workflows are triggered by:
- **Push to main**: Scan the main branch after merges
- **Pull requests**: Scan proposed changes before merge
- **Weekly schedule**: Catch new vulnerabilities in existing code (Monday 6am UTC)
- **Manual dispatch**: Run scans on-demand (govulncheck only)

## 📝 Notes

- All workflows use the latest stable Go version
- CodeQL and govulncheck results are uploaded in SARIF format for consistent viewing
- Dependabot checks run weekly and respect semantic versioning
- For public repositories, all these features are free
