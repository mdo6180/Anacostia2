import threading
from datetime import datetime, timedelta



def daily_scheduler(hour: int, minute: int, stop_event: threading.Event):
    """
    A generator that yields control at a specific time each day.
    Useful for implementing streams that need to perform actions at a specific time daily.

    Example usage:
    ```
    class CustomStream(Stream):
        def __iter__(self):
            # poll at 8:30 AM daily
            for _ in daily_scheduler(hour=8, minute=30, stop_event=self.stop_event):
                # Perform daily polling actions here
                yield artifact
    ```
    """

    while not stop_event.is_set():
        now = datetime.now()

        next_run = now.replace(
            hour=hour,
            minute=minute,
            second=0,
            microsecond=0
        )

        if now >= next_run:
            next_run += timedelta(days=1)

        seconds = (next_run - now).total_seconds()

        if stop_event.wait(timeout=seconds):
            break

        yield