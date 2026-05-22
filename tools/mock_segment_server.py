#!/usr/bin/env python3
"""Standalone mock Segment HTTP server for integration tests.

Accepts any POST request, returns {"success": true} with HTTP 200, and saves
each request body as a JSONL line (with a UTC timestamp) to an output file.

Also exposes two utility endpoints:
    GET /requests  - return all captured requests as a JSON array
    GET /reset     - clear the in-memory list and truncate the output file

Usage:
    python tools/mock_segment_server.py [--port PORT] [--output FILE] [--verbose]

Environment variables (overridden by CLI flags):
    MOCK_SEGMENT_PORT    Port to listen on (default: 8765)
    MOCK_SEGMENT_OUTPUT  Output JSONL file path (default: auto-created tempfile)
"""

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn


class MockSegmentHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)

        try:
            parsed_body = json.loads(body) if body else None
        except json.JSONDecodeError:
            parsed_body = body.decode(errors='replace')

        record = {
            'timestamp': datetime.now(tz=timezone.utc).isoformat(),
            'path': self.path,
            'body': parsed_body,
        }

        with open(self.server.output_file, 'a') as f:
            f.write(json.dumps(record) + '\n')

        self.server.captured.append(record)
        self._respond(200, {'success': True})

    def do_GET(self):
        if self.path == '/requests':
            self._respond(200, self.server.captured)
        elif self.path == '/reset':
            self.server.captured.clear()
            open(self.server.output_file, 'w').close()
            self._respond(200, {'reset': True})
        else:
            self._respond(404, {'error': 'not found'})

    def _respond(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        if self.server.verbose:
            super().log_message(format, *args)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a separate thread."""

    daemon_threads = True


def make_server(port=0, output_file=None, verbose=False):
    """Create and return a configured mock Segment server.

    Args:
        port: TCP port to bind (0 = OS picks a free port).
        output_file: Path for the JSONL output file (None = auto tempfile).
        verbose: Log every request to stderr.

    Returns:
        Tuple of (server, output_file_path).
    """
    if output_file is None:
        fd, output_file = tempfile.mkstemp(prefix='mock_segment_', suffix='.jsonl')
        os.close(fd)

    server = ThreadedHTTPServer(('', port), MockSegmentHandler)
    server.output_file = output_file
    server.captured = []
    server.verbose = verbose
    return server, output_file


def main():
    parser = argparse.ArgumentParser(description='Mock Segment HTTP server for integration tests')
    parser.add_argument('--port', type=int, default=int(os.getenv('MOCK_SEGMENT_PORT', '8765')))
    parser.add_argument('--output', default=os.getenv('MOCK_SEGMENT_OUTPUT'))
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    server, output_file = make_server(port=args.port, output_file=args.output, verbose=args.verbose)
    actual_port = server.server_address[1]

    print(f'Mock Segment server listening on http://0.0.0.0:{actual_port}', flush=True)
    print(f'Captured requests written to: {output_file}', flush=True)
    print(f'Inspect via: GET http://localhost:{actual_port}/requests', flush=True)
    print(f'Reset via:   GET http://localhost:{actual_port}/reset', flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down.', file=sys.stderr)
        server.server_close()


if __name__ == '__main__':
    main()
