"""
Log a status message.

This tiny module is here to prevent circular dependencies.
"""

from datetime import datetime


def log(msg):
    """Log a status message."""
    now = datetime.now().strftime('%Y-%M-%d %H:%M:%S')
    msg = f'{now} {msg}'
    print(msg)
