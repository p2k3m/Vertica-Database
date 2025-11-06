import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

if Path.cwd() != PROJECT_ROOT:
    os.chdir(PROJECT_ROOT)

os.environ.setdefault('ADMIN_USER', 'test-admin')
os.environ.setdefault('ADMIN_PASSWORD', 'test-password')
