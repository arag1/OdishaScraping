import numpy as np
import pandas as pd
import unittest
import warnings
from itertools import chain
import os
import re
import readline

import pandas
import datetime
from datetime import date
import numpy as np

import sqlalchemy as sqla

import time
from functools import wraps


def frange(start, stop, step):
    i = start
    while i < stop:
        yield i
        i += step


def orderedDict2pandasDf(odict):
    df = pd.DataFrame(columns = ['key', 'val'])
    for k, v in odict.iteritems():
       d = {'key': str(k), 'val': str(v)}
       df = df.append(d, ignore_index=True)
    return df



def parse_range(rng):
    parts = rng.split('-')
    if 1 > len(parts) > 2:
        raise ValueError("Bad range: '%s'" % (rng,))
    parts = [int(i) for i in parts]
    start = parts[0]
    end = start if len(parts) == 1 else parts[1]
    if start > end:
        end, start = start, end
    return range(start, end + 1)

def parse_range_list(rngs):
    return sorted(set(chain(*[parse_range(rng) for rng in rngs.split(',')])))


# return a pandas dataframe with a single row of None to match DB table schema
def getNoneDFRowFromDBTableSchema(tablename):
    try:
        import sys
        sys.path.append('../..')
        #from hqutilities.utils.bfefdb import *
        from hqutilities.utils import lmsdb
        from hqutilities.utils.lmsdb import LmsDb
        h = LmsDb('BFEF')
        s = h.Session()
        sql_stmt = "select * from " + tablename + " where 1=2"
        tableDF = pd.read_sql_query(sql_stmt, h.getEngine())
        ncols = tableDF.shape[1]
        dummyrow = [None] * ncols
        tableDF.loc[0] = dummyrow
        return tableDF
    except sqla.exc.SQLAlchemyError as e:
        logging.error("SQLALchemyError : {0}".format(e))
        raise
    finally:
        s.close()

# add microseconds to date column
def addMicroSeconds(column):
    formatString = "%Y-%m-%d %H:%M:%S.%f " + str(column)[-6:]
    timeStr = (column + datetime.timedelta(microseconds=000000)).strftime("%Y-%m-%d %H:%M:%S.%f %z")
    timeStr = timeStr[:-2]+':'+timeStr[-2:]
    return timeStr

def get_recursively(search_dict, field):
    """
    Takes a dict with nested lists and dicts,
    and searches all dicts for a key of the field
    provided.
    """
    fields_found = []

    for key, value in search_dict.iteritems():

        if key == field:
            fields_found.append(value)

        elif isinstance(value, dict):
            results = get_recursively(value, field)
            for result in results:
                fields_found.append(result)

        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    more_results = get_recursively(item, field)
                    for another_result in more_results:
                        fields_found.append(another_result)

    return fields_found





def retry(ExceptionToCheck, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry






def cut(x, bins, right=True):

    """
    ref: https://gist.github.com/jseabold/2296801
    Return indices of half-open bins to which each value of `x` belongs.
    Parameters
    ----------
    x : array-like
        Input array to be binned. It has to be 1-dimensional.
    bins : int or sequence of scalars
        If `bins` is an int, it defines the number of equal-width bins in the
        range of `x`. The range of `x`, however, is extended by .1% on each
        side to include the min or max values of `x`. If `bins` is a sequence
        it defines the bin edges allowing for non-uniform bin width.
    right : bool
        Indicates whether the bins include the rightmost edge or not. If
        right == True (the default), then the bins [1,2,3,4] indicate
        (1,2], (2,3], (3,4].
    Returns
    -------
    out : ndarray of ints
        Output array of indices, of same shape as `x`.
    """
    if not np.iterable(bins):
        if np.isscalar(bins) and bins < 1:
            raise ValueError("`bins` should be a positive integer.")
        if x.size == 0:
            # handle empty arrays. Can't determine range, so use 0-1.
            range = (0, 1)
        else:
            range = (x.min(), x.max())
        mn, mx = [mi+0.0 for mi in range]
        if mn == mx:
            mn -= 0.5
            mx += 0.5
        bins = np.linspace(mn, mx, bins+1, endpoint=True)
        bins[0] -= .1*mn
        bins[-1] += .1*mx
    else:
        bins = np.asarray(bins)
        if (np.diff(bins) < 0).any():
            raise AttributeError(
                    'bins must increase monotonically.')

    return np.digitize(x, bins, right)


class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """

    def __init__(self, methodName='runTest', param=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_klass, param=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, param=param))
        return suite