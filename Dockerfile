FROM ghcr.io/emb3rs-project/embers-base:latest as build

# creating the environment
COPY environment-py39.yml .
RUN --mount=type=cache,target=/opt/conda/pkgs mamba env create -f environment-py39.yml

# Installing Conda Pack
RUN --mount=type=cache,target=/opt/conda/pkgs conda install -c conda-forge conda-pack

# Use conda-pack to create a standalone enviornment
# in /venv:
RUN conda-pack -n gis-grpc-module -o /tmp/env.tar && \
    mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
    rm /tmp/env.tar

# We've put venv in same path it'll be in final image,
# so now fix up paths:
RUN /venv/bin/conda-unpack

FROM python:3.10-slim as runtime

# setup config
ENV GROUP_ID=1000 \
    USER_ID=1000

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

COPY --from=build /venv /venv

# Configuring app
WORKDIR /app
COPY . .

ENV PYTHONPATH=ms-grpc/plibs:module
ENV GRB_LICENSE_FILE=gurobi.lic

EXPOSE 50052

SHELL [ "/bin/bash", "-c" ]
ENTRYPOINT source /venv/bin/activate && \
    python -u server.py