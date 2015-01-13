# https://docs.python.org/3/library/exceptions.html
# https://docs.python.org/2/library/exceptions.html

class Exceptions:
    exceptions = [
        BaseException,
        Exception,
        # StandardError,
        ArithmeticError,
        BufferError,
        LookupError,

        AssertionError,
        AttributeError,
        EOFError,
        FloatingPointError,
        GeneratorExit,
        ImportError,
        IndexError,
        KeyError,
        KeyboardInterrupt,
        MemoryError,
        NameError,
        NotImplementedError,
        OSError,
        OverflowError,
        ReferenceError,
        RuntimeError,
        StopIteration,
        SyntaxError,
        IndentationError,
        TabError,
        SystemError,
        SystemExit,
        TypeError,
        UnboundLocalError,
        UnicodeError,
        UnicodeEncodeError,
        UnicodeDecodeError,
        UnicodeTranslateError,
        ValueError,
        ZeroDivisionError,
        EnvironmentError,
        IOError,
        # VMError,
        # WindowsError,

        BlockingIOError,
        ChildProcessError,
        ConnectionError,
        BrokenPipeError,
        ConnectionAbortedError,
        ConnectionRefusedError,
        ConnectionResetError,
        FileExistsError,
        FileNotFoundError,
        InterruptedError,
        IsADirectoryError,
        NotADirectoryError,
        PermissionError,
        ProcessLookupError,
        TimeoutError,

        Warning,
        UserWarning,
        DeprecationWarning,
        PendingDeprecationWarning,
        SyntaxWarning,
        RuntimeWarning,
        FutureWarning,
        ImportWarning,
        UnicodeWarning,
        BytesWarning,
        ResourceWarning
    ]

    @classmethod
    def get(cls, E):
        try:
            return cls.exceptions.index(E)
        except ValueError:
            pass

    @classmethod
    def throw(cls, n, args):
        try:
            E = cls.exceptions[n] # Other raise if n out of range
        except IndexError:
            E = Exception
        raise E(*args)

    @classmethod
    def add(cls, E):
        cls.exceptions.append(E)
