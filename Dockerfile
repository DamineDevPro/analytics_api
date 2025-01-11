FROM python:3.8.5-slim

RUN set -eux \
    && apt-get update \
    && apt-get -y install curl \
    && apt-get install -y gcc --no-install-recommends \
    python3-dev \
    # install nodejs and PM2
    && curl -sL https://deb.nodesource.com/setup_12.x | bash - \
    && apt-get -y install nodejs \
    && npm install pm2 -g \
    # cleanup
    && npm cache clean --force \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
# Set env for the project's working directory
ENV PROJECT_ROOT_DIR=/opt/py-analytics/

# Set config environment for the application
ENV PROJECT_ENV=production

# Specify the working directory in the container for the project
WORKDIR $PROJECT_ROOT_DIR

# Copy requirements.txt to the specified working directory.
COPY requirements.txt .

# Install project libraries from requirements.txt using pip3
RUN python3 -m venv venv
RUN . $PROJECT_ROOT_DIR/venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# Copy the project directory to the container's working directory
COPY . $PROJECT_ROOT_DIR

# Create project log directory
RUN mkdir -m 775 -p /var/log/app

EXPOSE 8098
CMD ["pm2-runtime", "ecosystem.config.js"]
