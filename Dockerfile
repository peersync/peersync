FROM debian:latest

RUN apt update && apt install -y iptables iproute2 curl

#ADD target/x86_64-unknown-linux-musl/debug/remake /usr/bin/remake
ADD target/x86_64-unknown-linux-musl/release/remake /usr/bin/remake

EXPOSE 8192 8192/udp

ENTRYPOINT ["/usr/bin/remake", "-c", "/etc/config.yaml", "-vvvvv"]
