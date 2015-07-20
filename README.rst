A library for running separated subprocesses with asyncio.

The usage is simple:

import asyncio

from aiopool import subprocess

@subprocess
def worker():
    # the code which should be runned in child process
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    
    # run 2 child processes
    workers = []
    for _ in range(2):
        workers.append(worker())
        
    loop.run_forever()
