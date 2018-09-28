# Taken from https://github.com/wkeeling/ratelimitingfilter because it's not available in PyPi as a package
from collections import defaultdict
import difflib
from functools import partial
import logging
import os
from time import time


class RateLimitingFilter(logging.Filter):
    """
    The RateLimitingFilter is a logging filter that can be used to throttle the number
    of records being sent through a logging handler. Internally the RateLimitingFilter is
    based on an implementation of the the Token Bucket algorithm to restrict the
    throughput of records. The desired throughput can be configured when instantiating
    an instance of the filter.
    """

    def __init__(self, rate=1, per=30, burst=1, **kwargs):
        """
        Create an instance of the RateLimitingFilter allowing a default rate of 1 record
        every 30 seconds when no arguments are supplied.

        :param rate: The number of records to restrict to, per the specified time interval. Default 1.
        :param per: The number of seconds during which 'rate' records may be sent. Default 30.
        :param burst: The maximum number of records that can be sent before rate limiting kicks in.
        :param kwargs: Additional config options that can be passed to the filter.
        """
        super(RateLimitingFilter, self).__init__()

        self.rate = rate
        self.per = per
        self.burst = burst

        self._default_bucket = TokenBucket(rate, per, burst)
        self._substr_buckets = None
        self._auto_buckets = None

        if 'match' in kwargs:
            if kwargs['match'] == 'auto':
                self._auto_buckets = defaultdict(partial(TokenBucket, rate, per, burst))
            else:
                self._substr_buckets = {}
                for s in kwargs['match']:
                    # Create a new token bucket for each substring
                    self._substr_buckets[s] = TokenBucket(rate, per, burst)

    def filter(self, record):
        """
        Determines whether the supplied record should be logged based upon current rate limits.

        If rate limits have been reached, the record is not logged and a counter is incremented
        to indicate that the record has been throttled. The next time a record is successfully
        logged, the number of previously throttled records is appended to the end of the message.

        :param record: The record to log.
        :return: True if the record can be logged, False otherwise.
        """

        bucket = self._bucket_for(record)

        if not bucket:
            return True

        if bucket.consume():
            if bucket.limited > 0:
                # Append a message to the record indicating the number of previously suppressed messages
                record.msg += '{linesep}... {num} additional messages suppressed'.format(linesep=os.linesep,
                                                                                         num=bucket.limited)
            bucket.limited = 0
            return True

        # Rate limit
        bucket.limited += 1
        return False

    def _bucket_for(self, record):
        if self._substr_buckets is not None:
            return self._get_substr_bucket(record)
        elif self._auto_buckets is not None:
            return self._get_auto_bucket(record)

        return self._default_bucket

    def _get_substr_bucket(self, record):
        # Locate the relevant token bucket by matching the configured substrings against the message
        for substr in self._substr_buckets:
            if substr in record.msg:
                return self._substr_buckets[substr]

        return None  # None indicates no filtering

    def _get_auto_bucket(self, record):
        if record.msg in self._auto_buckets:
            # We have an exact match - there is a bucket configured for this message
            return self._auto_buckets[record.msg]

        # Check whether we have a partial match - whether part of the message
        # matches against a token bucket we previously created for a similar message
        for msg, bucket in self._auto_buckets.items():
            matcher = difflib.SequenceMatcher(None, msg, record.msg)
            if matcher.ratio() >= 0.75:  # Might want to make the ratio threshold configurable?
                return bucket

        # No match, so create a new bucket for this message
        return self._auto_buckets[record.msg]


class TokenBucket(object):
    """
    An implementation of the Token Bucket algorithm.
    """

    def __init__(self, rate, per, burst):
        self._rate = rate
        self._per = per or 1
        self._burst = burst
        self._allowance = burst
        self._last_check = time()
        self.limited = 0

    def consume(self):
        """
        Attempts to consume a token from the bucket based upon the current state of the bucket.

        :return: True if a token can be consumed, False otherwise.
        """
        now = time()
        delta = now - self._last_check
        self._last_check = now

        self._allowance += delta * (self._rate / self._per)

        if self._allowance > self._burst:
            self._allowance = self._burst

        if self._allowance < 1:
            return False

        self._allowance -= 1
        return True

    def __repr__(self):
        return 'TokenBucket(rate={0._rate}, per={0._per}, burst={0._burst})'.format(self)
