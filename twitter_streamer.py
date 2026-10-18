#!/usr/bin/env python3
import socket, time, requests, json, sys

class TwitterStreamer:
    def __init__(self, bearer_token, port=7777):
        self.bearer_token = bearer_token; self._init_socket(port)
    def _init_socket(self, port):
        print('Awaiting client ...')
        s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', port)); s.listen(1)
        self.conn, addr = s.accept(); print(f'Connected from {addr}')
    def search(self, query, max_results=10):
        url = f'https://api.twitter.com/2/tweets/search/recent?query={query}&max_results={max_results}'
        r = requests.get(url, headers={'Authorization': f'Bearer {self.bearer_token}'})
        for t in r.json().get('data', []):
            self.conn.sendall((json.dumps(t)+'\n').encode())
    def stream(self, query, poll_interval=1.0):
        while True:
            try: self.search(query)
            except Exception as e: print(e)
            time.sleep(poll_interval)
if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: twitter_streamer.py <token> <query> <port>'); sys.exit(1)
    ts = TwitterStreamer(sys.argv[1], int(sys.argv[3])); ts.stream(sys.argv[2])
