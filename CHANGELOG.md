## 0.4.16 (2025-07-02)

### Fix

- correct source code copy path in Dockerfile

## 0.4.15 (2025-07-02)

### Fix

- update CMD instruction in Dockerfile to use module syntax

## 0.4.14 (2025-07-02)

### Fix

- update Docker build workflow trigger name in release.yaml

## 0.4.13 (2025-07-02)

### Refactor

- update GitHub Actions release workflow to include permissions and trigger Docker build

## 0.4.12 (2025-07-02)

### Fix

- update tag matching in GitHub Actions workflow to include all tags and versioned tags

## 0.4.11 (2025-07-02)

### Fix

- add '-at' flag to cz bump command in release workflow

## 0.4.10 (2025-07-02)

### Fix

- modify tag matching in GitHub Actions workflow to support all versions

## 0.4.9 (2025-07-02)

### Fix

- update regex for semantic version tag matching in GitHub Actions workflow

## 0.4.8 (2025-07-02)

### Fix

- update GitHub Actions workflow to trigger on semantic version tags

## 0.4.7 (2025-07-02)

### Fix

- update GitHub Actions workflow to trigger on all tags

## 0.4.6 (2025-07-02)

### Fix

- update Dockerfile to improve user permissions and streamline dependency installation

## 0.4.5 (2025-07-02)

### Fix

- update bind mounts in Dockerfile to use correct target paths

## 0.4.4 (2025-07-02)

### Fix

- add README.md binding in Dockerfile for dependency installation

## 0.4.3 (2025-07-02)

### Fix

- add workflow_dispatch trigger and rename job in Docker image publishing workflow

## 0.4.2 (2025-07-02)

### Fix

- update Docker image tag pattern to allow more flexible versioning

## 0.4.1 (2025-07-02)

### Fix

- update Docker image tag pattern in GitHub Actions workflow

## 0.4.0 (2025-07-02)

### Feat

- add GitHub Actions workflow for Docker image publishing to ghcr.io (#16)

## 0.3.2 (2025-07-02)

### Fix

- avoid index error when accessing lists (#15)

## 0.3.1 (2025-07-02)

### Fix

- load worker metadata defined twice (#14)

## 0.3.0 (2025-07-02)

### Feat

- add timeout purge for worker metrics (#13)

## 0.2.0 (2025-07-01)

### Feat

- enable metrics for worker status (#12)

## 0.1.0 (2025-06-27)

### Feat

- add GitHub action to generate tag (#10)
- enable metrics for task revoked and rejected (#9)
- enable metrics for task retried (#8)
- enable metrics for task failed (#7)
- enable metrics for task succeeded (#6)
- enhance task_started event handling in Roquefort class (#5)
- enable task received metrics (#4)
- add queue control inspection for detect queue linked by workers (#3)
- enhance Roquefort and HttpServer with graceful shutdown and improved metrics handling (#2)
- add event handler for celery events (#1)

### Fix

- update release workflow to push changes and tags
- update GitHub Actions git configuration for release workflow
