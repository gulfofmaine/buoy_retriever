FROM ghcr.io/prefix-dev/pixi:0.66.0 AS base

# Gather required base utils
RUN apt-get update && apt-get install -y \
        curl \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Output logging faster
ENV PYTHONUNBUFFERED=1
# Show deprecation warnings https://docs.djangoproject.com/en/2.2/howto/upgrade-version/#resolving-deprecation-warnings
ENV PYTHONWARNINGS=once

# User variables
ENV USER=uwsgi
ENV USER_HOME="/home/${USER}"
ENV USER_UID=988
ENV PIXI_HOME="${USER_HOME}/.pixi"

# Setup user
RUN groupadd ${USER} \
    && useradd -rm -d ${USER_HOME} -s /bin/bash -g ${USER} -u ${USER_UID} ${USER}
RUN mkdir -p "${USER_HOME}/app" /static /media /support
RUN chown -R ${USER}:${USER} ${USER_HOME} /static /media /support


USER ${USER}
WORKDIR ${USER_HOME}

COPY --chown=${USER}:${USER} ./pyproject.toml ./pixi.lock ${USER_HOME}
RUN --mount=type=cache,id=br_pixi,target=${USER_HOME}/.cache/rattler/cache,uid=${USER_UID},gid=${USER_UID} \
    pixi install -e default --frozen --manifest-path ./pyproject.toml

RUN echo '#!/bin/bash' > /support/shell-hook.sh \
    && pixi shell-hook -e default --shell bash | grep -vie "activate.d" | grep -vie "completion" >> /support/shell-hook.sh \
    && echo 'exec "$@"' >> /support/shell-hook.sh \
    && chmod +x /support/shell-hook.sh

ARG IMAGE_TAG
ENV IMAGE_TAG=${IMAGE_TAG:-unknown}

ENTRYPOINT ["/support/shell-hook.sh"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s \
    CMD /usr/bin/curl -D- -o /dev/null --silent --fail "http://localhost:8080/backend/health/"


FROM base AS develop


# FROM base AS deploy

# COPY --chown=${USER}:${USER} . ${USER_HOME}
# CMD ["uwsgi"l]
