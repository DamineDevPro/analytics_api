module.exports = {
  apps : [{
    name: 'py-analytics',
    script: 'manage.py',
    args: 'runserver 0.0.0.0:8098',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '2G',
    interpreter:'/opt/py-analytics/venv/bin/python3'
  }
  ]
};
