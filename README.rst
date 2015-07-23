A library for running separated subprocesses with asyncio. 

Library helps to run child processes. Each of them has its own event loop. Library supports fork and spawn methods for child creation. There some restrictions:
* If the fork method is used, event loop in child process will have shared resources with its parent. The main reason you want to use the fork method is creating a server which shares its sources with workers.  
* Spawn method creates absolutely separated subprocess. But as a target for spawn method should be used only separated function, not a class member or anonymous function. 

The main process looks after its children and restarts them if inactive or unresponsive. 

The usage is simple::

    import asyncio
    
    import aiopool
    
    @aiopool.fork # might be used @aiopool.spawn
    def worker():
        # the code which should be runned in child process
	# when it starts.
        
    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        
        # run 2 child processes
        workers = []
        for _ in range(2):
            workers.append(worker())
            
        loop.run_forever()
