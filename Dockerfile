FROM scratch
COPY ./output/sqlproxy /
ENTRYPOINT ["/sqlproxy"]