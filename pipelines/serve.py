#!/usr/bin/env python3
"""
Simple HTTP server with Range request support for PMTiles
"""
import os
from http.server import SimpleHTTPRequestHandler, HTTPServer


class RangeRequestHandler(SimpleHTTPRequestHandler):
    """HTTP handler with range request support"""

    def end_headers(self):
        # Add CORS headers
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Range')
        self.send_header('Access-Control-Expose-Headers', 'Content-Length, Content-Range')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def send_head(self):
        """Override to add range request support"""
        path = self.translate_path(self.path)

        if os.path.isdir(path):
            return super().send_head()

        try:
            f = open(path, 'rb')
        except OSError:
            self.send_error(404, "File not found")
            return None

        try:
            fs = os.fstat(f.fileno())
            file_size = fs.st_size

            # Check for Range header
            range_header = self.headers.get('Range')
            if range_header:
                # Parse range header (e.g., "bytes=0-1023")
                try:
                    byte_range = range_header.replace('bytes=', '').split('-')
                    start = int(byte_range[0]) if byte_range[0] else 0
                    end = int(byte_range[1]) if byte_range[1] else file_size - 1

                    if start >= file_size:
                        self.send_error(416, "Requested Range Not Satisfiable")
                        f.close()
                        return None

                    end = min(end, file_size - 1)
                    content_length = end - start + 1

                    self.send_response(206)  # Partial Content
                    self.send_header('Content-Type', self.guess_type(path))
                    self.send_header('Content-Length', str(content_length))
                    self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
                    self.send_header('Accept-Ranges', 'bytes')
                    self.end_headers()

                    # Seek to start and store the number of bytes to read
                    f.seek(start)
                    self._range_bytes = content_length
                    return f

                except (ValueError, IndexError):
                    # Invalid range header, ignore it
                    pass

            # No range request or invalid range - send full file
            self.send_response(200)
            self.send_header('Content-Type', self.guess_type(path))
            self.send_header('Content-Length', str(file_size))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
            self._range_bytes = None
            return f

        except Exception:
            f.close()
            raise

    def copyfile(self, source, outputfile):
        """Copy only the requested range"""
        # If this is a 206 response, only copy the requested bytes
        if hasattr(self, '_range_bytes') and self._range_bytes is not None:
            # Read and write in chunks to avoid memory issues
            bytes_left = self._range_bytes
            chunk_size = 64 * 1024  # 64KB chunks
            while bytes_left > 0:
                chunk = source.read(min(chunk_size, bytes_left))
                if not chunk:
                    break
                outputfile.write(chunk)
                bytes_left -= len(chunk)
        else:
            super().copyfile(source, outputfile)


def run_server(port=8000):
    """Run the HTTP server"""
    handler = RangeRequestHandler
    httpd = HTTPServer(('0.0.0.0', port), handler)
    print(f'Serving HTTP on 0.0.0.0 port {port} (http://localhost:{port}/) ...')
    print('Server supports HTTP Range Requests for PMTiles')
    print('Press Ctrl+C to stop')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down server...')
        httpd.shutdown()


if __name__ == '__main__':
    run_server()
