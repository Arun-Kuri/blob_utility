FROM python:3.10-slim AS base

WORKDIR /app
COPY . .

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=/app/requirements.txt \
    python -m pip install -r /app/requirements.txt

#EXPOSE 11002

ENTRYPOINT [ "python", "/app/blob_util.py"]