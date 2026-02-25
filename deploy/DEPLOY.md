# VPS Deployment Guide for Simon Papers

This guide covers deploying the application to `/var/www/tfang.info/html/simon`.

## Quick Deploy

```bash
# 1. Copy files to server (from local machine)
# Option A: Copy everything including the database (recommended if you have data)
rsync -avz --exclude '.git' --exclude 'venv' --exclude '__pycache__' --exclude '*.pyc' \
    --exclude '.env' --exclude 'pdfs' \
    /Users/tmf/code/search/simon_papers/ \
    youruser@tfang.info:/var/www/tfang.info/html/simon/

# Option B: Copy without database (if you want a fresh start on server)
rsync -avz --exclude '.git' --exclude 'venv' --exclude '__pycache__' --exclude '*.pyc' \
    --exclude '.env' --exclude 'pdfs' --exclude '*.db' \
    /Users/tmf/code/search/simon_papers/ \
    youruser@tfang.info:/var/www/tfang.info/html/simon/

# 2. SSH to server and set ownership
sudo chown -R www-data:www-data /var/www/tfang.info/html/simon

# 3. Set up Python environment
cd /var/www/tfang.info/html/simon
sudo -u www-data python3 -m venv venv
sudo -u www-data venv/bin/pip install --upgrade pip
sudo -u www-data venv/bin/pip install -r requirements.txt

# 4. Create .env file
sudo -u www-data cp .env.example .env
sudo -u www-data nano .env  # Edit with your API keys

# 5. Initialize database (skip if you copied it in step 1)
sudo -u www-data venv/bin/python run.py init

# 6. Install and start systemd service
sudo cp deploy/simon-papers.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable simon-papers
sudo systemctl start simon-papers

# 7. Check status
sudo systemctl status simon-papers
sudo journalctl -u simon-papers -f
```

## Environment Variables

Create `/var/www/tfang.info/html/simon/.env`:

```bash
# Required for AI analysis
DEEPSEEK_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here

# Server config
HOST=0.0.0.0
PORT=8001
URL_PREFIX=/simon

# Optional R2 config
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET_NAME=simon
R2_PUBLIC_URL=https://...
```

## File Permissions

```bash
# Restrict .env permissions
sudo chmod 600 /var/www/tfang.info/html/simon/.env
sudo chown www-data:www-data /var/www/tfang.info/html/simon/.env

# Ensure database is writable
sudo chown www-data:www-data /var/www/tfang.info/html/simon/db/simon_papers.db
```

## Updating the Application

```bash
cd /var/www/tfang.info/html/simon

# Pull/copy new code
git pull  # or rsync from local

# Update dependencies if needed
sudo -u www-data venv/bin/pip install -r requirements.txt

# Restart service
sudo systemctl restart simon-papers
```

## Troubleshooting

```bash
# Check logs
sudo journalctl -u simon-papers -f

# Test gunicorn manually
sudo -u www-data venv/bin/gunicorn --bind 127.0.0.1:8001 --env URL_PREFIX=/simon wsgi:application

# Check file permissions
ls -la /var/www/tfang.info/html/simon/
```
