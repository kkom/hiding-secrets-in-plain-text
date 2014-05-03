import datetime

def print_status(*args):
    """Output timestamp and space concatenated arguments to the terminal."""

    timestamp = str(datetime.datetime.now())
    print(" ".join((timestamp,) + tuple(map(str,args))))
