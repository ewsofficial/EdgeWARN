---
name: deployment-helper
description: Assist with deployment, CI/CD, and production operations.
---

# Deployment Helper Skill

This skill helps with deployment and production operations.

## Pre-Deployment Checklist

- [ ] All tests pass
- [ ] No console.log/print statements
- [ ] Environment variables configured
- [ ] Dependencies up to date
- [ ] Build succeeds
- [ ] Security audit passed

## Commands

### Build
```bash
# Node.js
npm run build
npm ci --production

# Python
pip install -r requirements.txt
```

### Process Management
```bash
# Screen sessions
screen -S app-name
screen -ls
screen -r app-name

# PM2 (Node.js)
pm2 start server.js
pm2 list
pm2 restart all
pm2 logs
```

### Health Checks
```bash
# Check if service is running
curl -f http://localhost:3000/health

# Check port usage
netstat -tlnp | grep 3000
lsof -i :3000
```

## Rollback

```bash
# Git rollback
git revert HEAD
git push

# Quick rollback
git checkout <previous-commit> -- .
```

## Monitoring

```bash
# System resources
htop
free -m
df -h

# Application logs
tail -f logs/app.log
journalctl -u app-service -f
```
