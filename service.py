#!/usr/bin/env python3
from operations import Operations

from datetime import datetime
import sys
import time


def get_week():
    # Start from Sunday so long operations that we do weekly start on
    # Sundays
    return int(datetime.now().strftime('%U'))


def get_day():
    return int(datetime.now().strftime('%j'))


def service():
    repeat_interval = 4 * 3600  # Run every 4 hours
    hashes_frequency = 3  # Check hashes every 3 days
    fail_sleep = 1800  # If failed, sleep for half an hour

    # Avoid doing long operations when starting the service, postpone
    # them to the next iteration
    db_recreated = get_week()
    hashes_checked = get_day()
    last_vacuum = get_day() - 1

    while True:
        # Notice that we recreate each time a new instance: in this way
        # we are sure that the Oauth session is refreshed at each run,
        # which should resolve some problems that I encountered
        # originally, when I created a single client before the while
        o = Operations()

        today = get_day()
        this_week = get_week()

        if last_vacuum != today:
            o.db.vacuum()
            last_vacuum = today

        try:
            if db_recreated != this_week:
                o.populate_db()
                db_recreated = this_week
        except:
            # You should think to something more clever
            print('Something failed', sys.exc_info())
            time.sleep(fail_sleep)
            continue

        check_hashes = (today - hashes_checked) > hashes_frequency

        try:
            o.compare_trees(check_hashes)
        except:
            # As above
            print('Something failed', sys.exc_info())
            time.sleep(fail_sleep)
            continue

        if check_hashes:
            hashes_checked = today

        o.db.commit()

        # We could check the start time, but some operations, like
        # database population are very slow. In that case, just
        # wait the usual time.
        time.sleep(repeat_interval)


if __name__ == '__main__':
    service()
