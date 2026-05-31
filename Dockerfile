FROM python:3.12-slim
WORKDIR /app

# Build the wheel from source and install it (with all extras).
COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir build \
 && python -m build --wheel \
 && pip install --no-cache-dir "$(ls dist/*.whl)[full]"

# Task file + dev submitter, used by the worker / submitter services.
COPY example_tasks.py submitter.py ./
