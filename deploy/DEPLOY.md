# Deploying Simon Papers to tfang.info/simon/

## Prerequisites

- Ubuntu/Debian VPS with nginx installed
- Domain pointing to your server (tfang.info)
- SSH access to the server

## Step 1: Copy files to server

From your local machine:

```bash
rsync -avz --exclude 'venv' --exclude '__pycache__' --exclude '*.pyc' \
    /Users/tmf/code/search/simon_papers/ \
    youruser@tfang.info:/var/www/tfang.info/html/simon/
```

Then fix ownership on the server:
```bash
sudo chown -R www-data:www-data /var/www/tfang.info/html/simon
```

## Step 2: Install system dependencies

SSH into your server and run:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv nginx

# For OCR support (optional)
sudo apt install -y tesseract-ocr poppler-utils
```

## Step 3: Set up Python environment

```bash
cd /var/www/tfang.info/html/simon

# Create virtual environment
sudo -u www-data python3 -m venv venv
sudo -u www-data venv/bin/pip install --upgrade pip
sudo -u www-data venv/bin/pip install -r requirements.txt
```

## Step 4: Set up the database

```bash
cd /var/www/tfang.info/html/simon
sudo -u www-data venv/bin/python run.py init
```

If you have an existing database, copy it:
```bash
scp db/simon_papers.db youruser@tfang.info:/var/www/tfang.info/html/simon/db/
sudo chown www-data:www-data /var/www/tfang.info/html/simon/db/simon_papers.db
```

## Step 5: Install systemd service

```bash
sudo cp /var/www/tfang.info/html/simon/deploy/simon-papers.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable simon-papers
sudo systemctl start simon-papers

# Check status
sudo systemctl status simon-papers
```

## Step 6: Configure nginx

Edit your existing nginx site config:

```bash
sudo nano /etc/nginx/sites-available/tfang.info
```

Add the location blocks inside your existing `server { }` block:

```nginx
server {
    server_name tfang.info;
    # ... your existing config ...

    # Add these lines:
    location /simon {
        proxy_pass http://127.0.0.1:8001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 60s;
        proxy_read_timeout 120s;
    }

    location /simon/static {
        alias /var/www/tfang.info/html/simon/web/static;
        expires 7d;
    }

    location /simon/pdf {
        alias /var/www/tfang.info/html/simon/pdfs;
        expires 1d;
    }
}
```

Test and reload nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## Step 7: Verify deployment

Visit: https://tfang.info/simon/

## Troubleshooting

### Check service logs
```bash
sudo journalctl -u simon-papers -f
```

### Restart the service
```bash
sudo systemctl restart simon-papers
```

### Test gunicorn manually
```bash
cd /var/www/tfang.info/html/simon
sudo -u www-data venv/bin/gunicorn --bind 127.0.0.1:8001 --env URL_PREFIX=/simon wsgi:application
```

### Check nginx error logs
```bash
sudo tail -f /var/log/nginx/error.log
```

### Permission issues
```bash
sudo chown -R www-data:www-data /var/www/tfang.info/html/simon
```

## Updating the application

1. rsync new files to server
2. Fix permissions and restart:
```bash
sudo chown -R www-data:www-data /var/www/tfang.info/html/simon
sudo systemctl restart simon-papers
```

If dependencies changed:
```bash
cd /var/www/tfang.info/html/simon
sudo -u www-data venv/bin/pip install -r requirements.txt
sudo systemctl restart simon-papers
```
