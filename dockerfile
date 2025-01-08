FROM python:3.10-slim AS base

#RUN apt update && apt install -y libsm6 libxrender1 libxext6 libgl1 libglib2.0-0 libxcb-xinerama0
WORKDIR /app
COPY . .

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=/app/requirements.txt \
    python -m pip install -r /app/requirements.txt

EXPOSE 11002

ENTRYPOINT [ "python", "/app/blob_util.py"]