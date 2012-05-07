#!/usr/bin/env python
import sys
import os
import json
import threading
import logging
import bz2
import time
import tweetstream

class StreamWriter(threading.Thread):
    def __init__(self, config, output):
        super(StreamWriter, self).__init__()
        self.tags = set(config['tags'])
        self.usr = config['user']
        self.pwd = config['password']
        self.out = bz2.BZ2File(output, 'w')
        self.changed = self.moved = self.stopped = False
        self.count = 0
        self.start_time = time.time()

    def run(self):
        while True:
            try:
                with tweetstream.FilterStream(self.usr, self.pwd, track=self.tags) as stream:
                    logging.info('New stream with tags [%s]', ' '.join(self.tags))
                    for tweet in stream:
                        self.out.write(json.dumps(tweet)+'\n')
                        self.count += 1
                        if self.changed or self.stopped or self.moved: break
            except tweetstream.ConnectionError:
                logging.info('Connection error, restarting...')
                time.sleep(1)

            if self.changed:
                self.changed = False
            if self.stopped:
                break
            if self.moved:
                self.out.close()
                self.out = bz2.BZ2File(self.moved, 'w')
                self.moved = False

        self.out.close()

    def info(self):
        rate = self.count / (time.time() - self.start_time)
        return '%d tweets @ %.3f tweets/s' % (self.count, rate)

def main(config, output):
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if os.path.exists(output):
        sys.stderr.write('Error: output path %s already existing.\n' % output)
        return

    with open(config) as f:
        config = json.loads(f.read())

    writer = StreamWriter(config, output)
    writer.start()
    try:
        while True:
            cmd = raw_input('> ').strip().split()
            if not cmd:
                continue
            if cmd[0] == 'exit':
                print('Exiting...')
                writer.stopped = True
                break
            elif cmd[0] == 'add' and len(cmd) > 1:
                for tag in cmd[1:]:
                    print('Adding tag %s' % tag)
                    writer.tags.add(tag)
                writer.changed = True
            elif cmd[0] == 'rm' and len(cmd) > 1:
                for tag in cmd[1:]:
                    print('Removing tag %s' % tag)
                    writer.tags.remove(tag)
                writer.changed = True
            elif cmd[0] == 'ls':
                print('Tags: [%s]' % ' '.join(writer.tags))
            elif cmd[0] == 'info':
                print('Summary: %s' % writer.info())
            elif cmd[0] == 'mv' and len(cmd) == 2 and cmd[1].strip():
                print('Moving output to %s' % cmd[1])
                writer.moved = cmd[1]
            elif cmd[0] == 'help':
                print('Available commands:\n'
                      'add: add new tags to track\n'
                      'rm: stop tracking given tags\n'
                      'ls: list the tracked tags\n'
                      'info: output crawling statistics\n'
                      'exit: stop the crawler')
            else:
                print('Error: cannot understand command!')
    except EOFError:
        print('Exiting...')
        writer.stopped = True

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s config.json output.bz2\n' % sys.argv[0])
        sys.exit(1)
    main(*sys.argv[1:])
