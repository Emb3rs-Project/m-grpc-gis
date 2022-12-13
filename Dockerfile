ARG BASE_TAG=latest

FROM ghcr.io/emb3rs-project/embers-base:${BASE_TAG} as build

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

ARG COPT_KEY

# setup config
ENV GROUP_ID=1000 \
    USER_ID=1000

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

# Copy dependencies
COPY --from=build /venv /venv

# Copy COPT
COPY --from=build /opt/copt60 /opt/copt60

# Configuring app
WORKDIR /app
COPY . .

# Submodules config
ENV PYTHONPATH=ms-grpc/plibs:module

# Gurobi config
ENV GRB_LICENSE_FILE=gurobi.lic

# COPT config
ENV COPT_HOME=/opt/copt60
ENV COPT_LICENSE_FILE=copt.lic \
    COPT_LICENSE_DIR=/opt/copt60 \
    PATH=$COPT_HOME/bin:$PATH \
    LD_LIBRARY_PATH=$COPT_HOME/lib:$LD_LIBRARY_PATH \
    PYTHONPATH=$PYTHONPATH:$COPT_HOME/lib/pyomo

RUN if [[ -n "${COPT_KEY}" ]] ; then copt_licgen -key ${COPT_KEY} ; else copt_licgen -file $COPT_LICENSE_FILE ; fi

EXPOSE 50052

SHELL [ "/bin/bash", "-c" ]
RUN echo 'source /venv/bin/activate' >> ~/.bashrc && source ~/.bashrc

ENTRYPOINT source /venv/bin/activate && \
    python -u server.py