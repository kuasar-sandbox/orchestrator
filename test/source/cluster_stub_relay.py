#!/usr/bin/env python3
"""Opaque TCP relay for the process-owner failure case; never parses NodeLink.

Each new connection reads the current target file. The test can change the
transport endpoint after killing a Registry without restarting the Node or
changing its durable state. Existing connections keep their original target.
"""
import argparse
import socket
import threading
from pathlib import Path


def pump(source, target):
    try:
        while data := source.recv(65536):
            target.sendall(data)
    except OSError:
        pass
    finally:
        try:
            target.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def relay(client, target_file):
    with client:
        try:
            host, port = target_file.read_text().strip().rsplit(":", 1)
            target = socket.create_connection((host, int(port)), timeout=2)
        except (OSError, ValueError):
            return
        with target:
            target.settimeout(None)
            upstream = threading.Thread(target=pump, args=(client, target), daemon=True)
            upstream.start()
            pump(target, client)
            upstream.join()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--target-file", type=Path, required=True)
    args = parser.parse_args()
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", args.port))
        listener.listen()
        while True:
            client, _ = listener.accept()
            threading.Thread(target=relay, args=(client, args.target_file), daemon=True).start()


if __name__ == "__main__":
    main()
