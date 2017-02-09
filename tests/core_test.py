# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (C) 2017, Ryan P. Wilson
#
#      Authority FX, Inc.
#      www.authorityfx.com

from afxthreads.core import MultiProcessor, MultiThreader
import math
from time import time


def trace_test():
    raise Exception("Exception raised from exception callback")


def exc_callback(e):
    trace_test()


def f(i):
    for r in xrange(1000000):
        x = int(math.ceil(math.pow(0.5, 2)))
    # Test error handling by rasing an error in some work
    if i == 0:
        raise Exception('This is a test error to show traceback functionality')
    return x + i


s = time()
print 'Testing MultiThreader with exception logging'
with MultiThreader(log_exceptions=True) as mt:
    for i in range(8):
        print "Adding work - " + str(f) + ', args=' + str(i)
        mt.add_work(f, (i,))

    print 'Waiting for work to complete...'
    mt.wait()

    mt_time = time() - s
    print 'Elapsed time = %.2f seconds' % (mt_time,)
    print 'Results = ' + str(sorted(mt.results))

print '\nTesting MultiProcessor with 2 processes'
with MultiProcessor(processes=2, log_exceptions=True) as mp:
    for i in range(8):
        print "Adding work - " + str(f) + ', args=' + str(i)
        mp.add_work(f, (i,))

    while mp.is_working():
        print 'Waiting on one...'
        mp.wait_one()
        print 'done one, testing mp.abort()'
        mp.abort()

    print 'mp.state() = ' + str(mp.state())
    print 'Results = ' + str(sorted(mp.results))

    print 'Initializing mt with default processes'
    s = time()
    mp.initialize()
    print 'mp.state() = ' + str(mp.state()) + ' with ' + str(mp.processes()) + ' processes'
    for i in range(8):
        print "Adding work with exception traceback and callback - " + str(f) + ', args=' + str(i)
        mp.add_work(f, (i,), exc_traceback=True, exc_callback=exc_callback)

    print 'Waiting for work to complete...'
    mp.wait_all()

    mp_time = time() - s
    print 'Elapsed time = %.2f seconds' % (mp_time,)
    print 'Results = ' + str(sorted(mp.results))

    print '\nMultiProcessor completed the same work %.2fx faster than MultiThreader' % (mt_time / mp_time,)
