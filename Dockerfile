FROM scratch

COPY ./poloniex-script /app/poloniex-script
COPY ./ca-certificates.crt /etc/ssl/certs/

WORKDIR /app

ENTRYPOINT ["/app/poloniex-script"]