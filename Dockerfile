FROM adoptopenjdk/openjdk11:x86_64-alpine-jre-11.0.12_7
ARG version=1.0.0
ADD ./build/distributions/*.tar /usr/src/ClickHouse1CTechJournal
WORKDIR /usr/src/ClickHouse1CTechJournal/ClickHouse1CTechJournal-${version}/bin
RUN chmod +x ClickHouse1CTechJournal
CMD ["./ClickHouse1CTechJournal", "-d", "/var/lib/ClickHouse1CTechJournal/tj"]
VOLUME /var/lib/ClickHouse1CTechJournal/tj