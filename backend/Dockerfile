FROM ghcr.io/prefix-dev/pixi:0.55.0-noble@sha256:423daf63e4e876ab42b1be9ddee3fc867ba8b9fa511e0634de3b9df12ff1a90d AS base

# Output logging faster
ENV PYTHONUNBUFFERED=1
# Show deprecation warnings https://docs.djangoproject.com/en/2.2/howto/upgrade-version/#resolving-deprecation-warnings
ENV PYTHONWARNINGS=once

# User variables
ENV USER=uwsgi
ENV USER_HOME="/home/${USER}/"
ENV USER_UID=1001

# Setup user
RUN groupadd ${USER} \
    && useradd -rm -d ${USER_HOME} -s /bin/bash -g ${USER} -u ${USER_UID} ${USER}
RUN mkdir -p /static /media /support
RUN chown -R ${USER}:${USER} ${USER_HOME} /static /media /support


USER ${USER}
WORKDIR ${USER_HOME}

COPY --chown=${USER}:${USER} ./pyproject.toml ./pixi.lock ${USER_HOME}
RUN --mount=type=cache,id=br_pixi,target=${USER_HOME}.cache/rattler/cache,uid=${USER_UID},gid=${USER_UID} \
    pixi install -e default

RUN pixi shell-hook -e default > /support/shell-hook.sh \
    && echo 'exec "$@"' >> /support/shell-hook.sh

ARG IMAGE_TAG
ENV IMAGE_TAG=${IMAGE_TAG:-unknown}

ENTRYPOINT ["/bin/bash", "/support/shell-hook.sh"]


FROM base AS develop


# FROM base AS deploy

# COPY --chown=${USER}:${USER} . ${USER_HOME}
# CMD ["uwsgi"l]
