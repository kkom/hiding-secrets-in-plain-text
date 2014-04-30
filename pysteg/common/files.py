import contextlib
import os

class FileAlreadyProcessed(Exception):
    """
    Exception to be raised by the context if the file has already been
    processed. The context manager will discard the exception and continue to
    close the file.
    """
    pass

@contextlib.contextmanager
def open_file_to_process(path, mode):
    """
    A context manager for working with files or directories that need to be
    processed.

    It is a wrapper around the standard open() function. Before yielding the
    file opened in a specified mode, it checks if it has already been processed
    by looking for a special flag file.

    It the file has already been processed, the context manager yields False to
    the context. The context is then expected to raise the FileAlreadyProcessed
    exception, which the manager will ignore.

    If the file has not been processed, the context manager will yield the
    opened file and then create the flag upon closing the context.

    Note 1: This is NOT a way to handle multiprocessing, just restarting jobs.

    Note 2: This is very ugly and possible an abuse of tools, but works for now.
    """

    flag_path = path_append_flag(path, "_DONE")

    if os.path.exists(flag_path):
        try:
            yield False
        except FileAlreadyProcessed:
            pass
    else:
        with open(path, mode) as f:
            yield f

        open(flag_path, "w").close()

def path_append_flag(path, flag, hidden=True):
    """
    Appends a flag to the end of a path. By default the basename directory or
    file is also hidden.
    """

    # os.path.split splits the path into a (most, last) tuple. "most" is the
    # dirname and "last" is the the basename. If the path leads to a directory
    # and happens to have a trailing slash, basename will be "/" (unlike in the
    # unix program basename). os.path.normpath eliminates this behaviour by
    # removing the trailing slash.

    split_path = os.path.split(os.path.normpath(path))
    hide_character = "." if hidden else ""

    return os.path.join(split_path[0], hide_character + split_path[1] + flag)
