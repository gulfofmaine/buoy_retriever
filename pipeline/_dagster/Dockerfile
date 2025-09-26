FROM ghcr.io/prefix-dev/pixi:0.55.0-noble@sha256:423daf63e4e876ab42b1be9ddee3fc867ba8b9fa511e0634de3b9df12ff1a90d AS base

ENV PYTHON_USER=ioos
ENV HOME=/home/${PYTHON_USER}
ENV DAGSTER_HOME=${HOME}

RUN mkdir ${HOME}

WORKDIR ${HOME}

COPY ./pixi.toml ./pixi.lock ${HOME}/
RUN --mount=type=cache,id=br_dagster,target=${HOME}.cache/rattler/cache \
    pixi install -e default

RUN pixi shell-hook -e default > /shell-hook.sh \
    && echo 'exec "$@"' >> /shell-hook.sh

ENTRYPOINT ["/bin/bash", "/shell-hook.sh"]

COPY ./dagster.yaml ./workspace.yaml ${HOME}/

EXPOSE 3002

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3002", "-w", "workspace.yaml"]
