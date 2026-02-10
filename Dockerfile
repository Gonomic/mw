FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Copy start script
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Expose ports (app + debug)
EXPOSE 8000 5678

# Entrypoint script handles debug mode
CMD ["/start.sh"]
