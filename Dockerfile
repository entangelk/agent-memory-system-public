FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml /tmp/pyproject.toml

RUN python -c "import pathlib, subprocess, sys, tomllib; data = tomllib.loads(pathlib.Path('/tmp/pyproject.toml').read_text()); deps = list(data['project'].get('dependencies', [])); deps.extend(data['project'].get('optional-dependencies', {}).get('dev', [])); subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--no-cache-dir', *deps])"

COPY . /app

CMD ["python", "-m", "src.server"]
