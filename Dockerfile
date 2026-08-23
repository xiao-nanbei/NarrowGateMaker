FROM python:3.11-slim

ARG NARROWGATE_INSTALL_TARGET=.

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        ninja-build \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY pyproject.toml README.md LICENSE ./
COPY narrowgate ./narrowgate
COPY data_paths.py data_quality.py calendar_features.py market_fusion.py ./
COPY data ./data
COPY execution ./execution
COPY features ./features
COPY live ./live
COPY models ./models
COPY research ./research
COPY strategy ./strategy
COPY examples ./examples
COPY scripts ./scripts
COPY tests ./tests

RUN python -m pip install --upgrade pip \
    && python -m pip install -e "${NARROWGATE_INSTALL_TARGET}"

CMD ["narrowgate", "doctor"]
