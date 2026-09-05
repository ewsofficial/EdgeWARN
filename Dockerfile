FROM continuumio/miniconda3:25.3.1-1

WORKDIR /tmp/edgewarn-build

# environment.yml remains the only runtime dependency authority.  The wheel is
# built and installed without dependency resolution so pip cannot create a
# second, divergent runtime environment.
COPY environment.yml pyproject.toml README.md ./
COPY src ./src
COPY config ./config
RUN conda env create --file environment.yml \
    && /opt/conda/envs/EdgeWARN/bin/python -m pip wheel \
        --no-deps --no-build-isolation --wheel-dir /tmp/edgewarn-wheel . \
    && /opt/conda/envs/EdgeWARN/bin/python -m pip install \
        --no-deps /tmp/edgewarn-wheel/edgewarn_core-*.whl \
    && mkdir -p /etc/edgewarn \
    && cp -a config /etc/edgewarn/config \
    && conda clean --all --yes \
    && rm -rf /tmp/edgewarn-build /tmp/edgewarn-wheel

ENV PATH="/opt/conda/envs/EdgeWARN/bin:${PATH}" \
    EDGEWARN_BASE_DIR="/var/lib/edgewarn"

WORKDIR /opt/edgewarn
VOLUME ["/var/lib/edgewarn"]
STOPSIGNAL SIGTERM

ENTRYPOINT ["edgewarn"]
CMD ["run", "--config-path", "/etc/edgewarn/config"]
