FROM hub.cloud.ctripcorp.com/juicedata/run-base:latest

RUN mkdir -p /opt/container
COPY --from=project juicefs /usr/local/bin/
COPY --from=project pkg/proxy/entrypoint.sh /opt/container/

RUN ln -s /usr/local/bin/juicefs /bin/mount.juicefs && ln -s /usr/local/bin/juicefs /bin/juicefs && \
    chmod +x /opt/container/entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
