import time

class timer:
    """A wrapper class to time a block of code."""
    
    def __init__(self, method="clock"):
        time_functions = {
            "clock": time.clock,
            "time": time.time,
        }
    
        self.time = time_functions[method]
    
    def __enter__(self):
        self.start = self.time()
        return self

    def __exit__(self, *args):
        self.end = self.time()
        self.interval = self.end - self.start
        