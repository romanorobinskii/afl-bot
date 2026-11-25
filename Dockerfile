FROM python:3.11-slim-bookworm

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install --with-deps --only-shell chromium \
    && rm -rf /var/lib/apt/lists/*

COPY afl_bot.py moex-root-ca.pem ./
CMD ["python", "-u", "afl_bot.py"]
