import socket
import tempfile
import threading
import unittest
from pathlib import Path

from proxy_lifecycle_client import Probe


class LifecycleManagementTest(unittest.TestCase):
    def test_unix_socket_path_is_not_the_http_host(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'stats.sock')
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(path)
                server.listen(1)
                server.settimeout(3)
                received = []

                def respond():
                    with server.accept()[0] as connection:
                        connection.settimeout(3)
                        request = b''
                        while b'\r\n\r\n' not in request:
                            chunk = connection.recv(4096)
                            if not chunk:
                                return
                            request += chunk
                        received.append(request)
                        connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}')

                worker = threading.Thread(target=respond)
                worker.start()
                try:
                    probe = Probe.__new__(Probe)
                    probe.stats = path
                    self.assertEqual(probe.management()[::2], (200, b'{}'))
                finally:
                    worker.join(4)
                self.assertFalse(worker.is_alive())
                self.assertEqual(len(received), 1)
                self.assertIn(b'GET /status HTTP/1.1\r\n', received[0])
                self.assertIn(b'Host: localhost\r\n', received[0])
                self.assertNotIn(path.encode(), received[0])


if __name__ == '__main__':
    unittest.main()
