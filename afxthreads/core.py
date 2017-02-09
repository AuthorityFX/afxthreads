# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (C) 2017, Ryan P. Wilson
#
#      Authority FX, Inc.
#      www.authorityfx.com

import multiprocessing
import threading
import Queue
import traceback
import sys


class ErrorHandler(object):
    """Callable wrapper class to catch exceptions and provide traceback. When an
    exception is raised in pool.apply_async the resutls callback does not get
    called.  This class handles the exception to ensure apply_async will return
    normally and notify ResultsLog via callback that work has completed allowing
    any blocking wait calls to proceed.

    Args:
        f (callable): Called with exception handling.
        exc_traceback(bool): Print exception information.
        exc_callback(callable): Called on exception.

    """
    def __init__(self, f, exc_traceback=False, exc_callback=None):
        self.__callable = f
        self.__exc_callback = exc_callback
        self._exc_traceback = exc_traceback

    def __call__(self, *args, **kwargs):
        """Call self.__callable.  On exception, self.__exc_callback will be
        called and traceback.print_exception() will provide debugging help.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Returns the return of self.__callable.  If an exception is handled,
            returns None

        """
        result = None
        try:
            result = self.__callable(*args, **kwargs)
        except Exception as e:
            result = e
            if self._exc_traceback:
                ei = sys.exc_info()
                # Prune "result = self.__callable(*args, **kwargs)" from traceback
                traceback.print_exception(ei[0], ei[1], ei[2].tb_next)
            else:
                sys.exc_clear()

            if self.__exc_callback is not None:
                try:
                    self.__exc_callback(e)
                except:
                    if self._exc_traceback:
                        ei = sys.exc_info()
                        # Prune "result = self.__callable(*args, **kwargs)" from traceback
                        traceback.print_exception(ei[0], ei[1], ei[2].tb_next)
                    else:
                        sys.exc_clear()

        return result


class ResultsLog(object):
    """Thread-safe wrapper class to log multiprocessing.Pool.apply_async()
    results from callback.
    """
    def __init__(self, log_exceptions=False):
        self._counter = 0
        self._log_event = threading.Event()
        self._rlock = threading.RLock()
        self._waiting = False
        self._log_exceptions = log_exceptions
        self._results = []

    def log(self, result=None):
        """Decrement _counter, append result to list, abd set _logged event."""
        with self._rlock :
            if self._counter > 0:
                self._counter -= 1
            if result is not None or (issubclass(type(result), Exception) and self._log_exceptions):
                self._results.append(result)
            if self._waiting:
                self._log_event.set()

    def add_work(self):
        """Increment _counter and clear log event to allow wait to block"""
        with self._rlock :
            self._counter += 1
            self._log_event.clear()

    def wait_one(self, timeout=None):
        """Block until one result is logged"""
        with self._rlock:
            self._waiting = True
        self._log_event.wait(timeout)
        with self._rlock:
            self._waiting = False
            if self.results_pending():
                self._log_event.clear()

    def wait_all(self, timeout=None):
        """Block until the number of calls made to log are equal to the number
        of calls to add_work.
        """
        while self.results_pending():
            self.wait_one(timeout)

    def results_pending(self):
        """Non-blocking call to check if all pending results have been logged"""
        return self._counter is not 0

    def reset_counter(self):
        """Set event and zero counter to ensure wait calls will not block"""
        with self._rlock :
            self._counter = 0
            self._log_event.set()

    def reset(self):
        """Resest to initialized state"""
        with self._rlock :
            self.reset_counter()
            self.results = []

    @property
    def results(self):
        return self._results

    @results.setter
    def results(self, results):
        with self._rlock:
            self._results = results

    @property
    def log_exceptions(self):
        return self._log_exceptions

    @log_exceptions.setter
    def log_exceptions(self, value):
        with self._rlock:
            self._log_exceptions = value


class MultiProcessor(object):
    """MultiProcessor creates a process pool of multiprocessing.Process objects.

    Args:
        processes (int): Desired number of processes to launch. Defaults to 0
        which launches t processes as returned by multiprocessing.cpu_count()

    Process uses os.Fork(). On Linux, os.Fork() calls fork() which creates a new
    process by duplicating the calling process. Under Linux, fork() is
    implemented using copy-on-write pages, so the only penalty that it incurs is
    the time and memory required to duplicate the parent's page tables, and to
    create a unique task structure for the child.

    Use a context manager to ensure all processes are terminated when no longer
    needed.  Otherwise, dispose() must be explicitly called to shutdown all
    running processes.

    Example:
        >>> with MultiProcessor() as mp:
        ...     mp.add_work(f, args)
        ...     mp.join()
        ...     print mp.results()

        >>> mp = MultiProcessor()
        >>> try:
        ...     mp.add_work(f, args)
        ...     mp.join()
        ...     print mp.results()
        >>> except Exception, e:
        ...     pass
        >>> finally:
        ...     mp.dispose()
        ...     del mp

    """

    def __init__(self, processes=None, log_exceptions=False):
        self._running = False
        self.initialize(processes, log_exceptions)

    def __enter__(self):
        return self

    def initialize(self, processes=None, log_exceptions=False):
        if not self._running:
            self._results = ResultsLog(log_exceptions)
            avail_cores = multiprocessing.cpu_count()
            self._num_processes = min(processes, avail_cores) if processes is not None else avail_cores
            self._pool = multiprocessing.Pool(self._num_processes)
            self._running = True

    def add_work(self, f, args=(), kwargs={}, exc_traceback=False, exc_callback=None):
        """Non-blocking function that puts a callable into work queue to be
        digested and called by the first available process in process pool.

        Args:
            f (callable): A callable to be called by a worker in process pool.
            args (tuple): Defaults to empty tuple (). Arguments to be passed to
                f when called.
            kwargs (dict): Defaults to empty dictionary. Arbitrary keyword
                arguments passed to f.
            exc_callback(callable): Called when exception is handled by worker.

        Unlike threading.Thread, processes do not share memory.  There are two
        ways to allow processes to use shared data -- shared memory, and
        managers.  Shared memory such as multiprocessing.Value must be passed to
        the process on creation as it cannot be pickled in a Queue object.  For
        this general multiprocessor, Managers must be used.

        Note:
            Only mangers member functions can be accessed from pool processes.
        Member data is immutiable. In the example below, the change function
        will successfully acces my_class.value and modify my_value.value;
        however, my_class member data can be changes even via setter functions.

        Example:
            >>> import multiprocessing
            >>> from multiprocessing.managers import BaseManager
            >>>
            >>> def change(my_class, counter, lock):
            ...     temp = counter.value
            ...     with lock:
            ...         counter.value *= my_class.get_value()
            ...     my_class.set_value(temp)

            >>> def accumulate(counter, lock):
            ...     with lock:
            ...         counter.value += 1
            ...
            >>> class MyClass(object):
            ...     def __init__(self):
            ...         self._value = 2
            ...     def get_value(self):
            ...         return self._value
            ...     def set_value(self, value):
            ...         self._value = value

            >>> manager = multiprocessing.Manager()
            >>> counter = manager.Value('i', 0)
            >>> lock = manager.Lock()
            ...
            >>> my_class = MyClass()
            >>> class MyManager(BaseManager): pass
            >>> MyManager.register('my_class', lambda:my_class)
            >>> class_manager = MyManager()
            >>> class_manager.start()
            ...
            >>> print my_class.get_value(), counter.value
            2 0
            >>> with afx_threading.MultiProcessor() as mp:
            ...     for i in range(20):
            ...         mp.add_work(accumulate, (counter, lock))
            ...     mp.join()
            ...     mp.add_work(change, (my_class, counter, lock))
            ...     mp.join()
            ...
            >>> manager.shutdown()
            >>> class_manager.shutdown()
            ...
            >>> print my_class.get_value(), counter.value
            2 40

        """
        if self._running:
            self._results.add_work()
            self._pool.apply_async(ErrorHandler(f, exc_traceback, exc_callback), args, kwargs, callback=self._results.log)

    @property
    def results(self):
        return self._results.results

    @results.setter
    def results(self, results):
        self._results.results = results

    @property
    def log_exceptions(self):
        return self._results.log_exceptions

    @log_exceptions.setter
    def log_exceptions(self, value):
        self._results.log_exceptions = value

    def state(self):
        """Returns True if running, otherwise False"""
        return self._running

    def is_working(self):
        """Returns True if results are pending."""
        return self._results.results_pending()

    def processes(self):
        """Returns the number of processing in process pool."""
        return self._num_processes

    def wait_one(self, timeout=None):
        """Blocks until one task from queue has been completed."""
        self._results.wait_one(timeout)

    def wait_all(self, timeout=None):
        """Blocks until all work added to queue has been completed."""
        self._results.wait_all(timeout)

    def abort(self):
        """Terminate process pool, set running to False, and reset work counter"""
        self._running = False
        self._results.reset_counter()
        self._pool.terminate()
        self._pool.join()

    def dispose(self):
        """Prevents any more tasks from being submitted to the pool and blocks
        until all work has been completed.

        """
        self._running = False
        self._pool.close()
        self._results.reset_counter()
        self._pool.join()

    def __exit__(self, type, value, traceback):
        self.dispose()

    def __del__(self):
        self.dispose()


class MultiThreader(object):
    """Threader creates a pool of threading.Thread objects.  Threads do not
    execute code concurrently due to the global interpreter lock (GIL). Useful
    for I/O-bound applications.

    Args:
        threads (int): Desired number of threads to launch. Defaults to 0
        which will launch as many threads as returned by
        multiprocessing.cpu_count()

    Use a context manager to ensure all threads are terminated when no longer
    needed.  Otherwise, dispose() must be explicitly called to shutdown all
    running threads.

    Example:
        >>> with Threader() as t:
        ...     t.add_work(f, args)
        ...     t.join()

        >>> t = Threader()
        >>> try:
        ...     t.add_work(f, args)
        ...     t.join()
        >>> except Exception, e:
        ...     pass
        >>> finally:
        ...     t.dispose()
        ...     del mp

    """

    def __init__(self, threads=None, log_exceptions=False):
        self._work_queue = Queue.Queue()
        self._lock = threading.Lock()
        self._results = []
        self._log_exceptions = log_exceptions
        avail_cores = multiprocessing.cpu_count()
        num_threads = min(threads, avail_cores) if threads is not None else avail_cores
        self._thread_pool = []
        for t in range(num_threads):
            self._thread_pool.append(threading.Thread(target=self.__worker))
            self._thread_pool[-1].daemon = True
            self._thread_pool[-1].start()

    def __enter__(self):
        return self

    def __worker(self):
        while True:
            try:
                result = None
                w = self._work_queue.get()
                if w == None:
                     break
                f = w[0]
                args = w[1]
                kwargs = w[2]
                result = f(*args, **kwargs)
            except Exception as e:
                # Prune "self.__exc_callable(e)" from traceback
                ei = sys.exc_info()
                traceback.print_exception(ei[0], ei[1], ei[2].tb_next)
                if self._log_exceptions:
                    result = e
            finally:
                with self._lock:
                    if result is not None:
                        self._results.append(result)
                try:
                    # Will throw ValueError if called more times than there were
                    # items placed in the queue. This should be impossible since
                    # this code will not run unless self._work_queue.get()
                    # returns.
                    self._work_queue.task_done()
                except ValueError:
                    pass

    def add_work(self, f, args=(), kwargs={}):
        """Non-blocking function that puts a callable into work queue to be
        digested and called by the first available thread in thread pool.

        Args:
            f (callable function): A callable to be called by a worker in thread
                pool.
            args (tuple): Defaults to empty tuple (). Arguments to be passed to
                f when called.
            kwargs (dict): Defaults to empty dictionary. Arbitrary keyword
                arguments passed to f.

        """
        self._work_queue.put((f, args, kwargs))

    @property
    def results(self):
        with self._lock:
            return self._results

    @results.setter
    def results(self, results):
        with self._lock:
            self._results = results

    @property
    def log_exceptions(self):
        with self._rlock:
            return self._log_exceptions

    @log_exceptions.setter
    def log_exceptions(self, value):
        with self._rlock:
            self._log_exceptions = value

    def wait(self):
        """Blocks until all work added to work queue has been completed."""
        self._work_queue.join()

    def dispose(self):
        """Signal _worker function to break by adding None object to queue.  Join
        queue and wait for threads to exit.
        """
        for t in self._thread_pool:
            if t.is_alive():
                self._work_queue.put(None)
        self.wait()

    def __exit__(self, type, value, traceback):
        self.dispose()

    def __del__(self):
        self.dispose()
