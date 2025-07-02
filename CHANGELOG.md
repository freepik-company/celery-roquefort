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
