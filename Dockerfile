# Shopping Sessions: one static C++ binary in an empty image.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ libc6-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/ /src/
RUN g++ -std=c++20 -O3 -march=x86-64-v3 -mtune=cascadelake -pthread -static \
        -o /ss main.cpp && strip /ss

FROM scratch
COPY --from=build /ss /ss
USER 1000:1000
CMD ["/ss"]
