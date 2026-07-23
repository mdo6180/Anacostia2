import os
import signal
import sys
from logging import Logger
import debugpy



def stop_if(
    *,
    current_run: int,
    current_iter: int,
    target_run: int,
    target_iter: int,
    mode: str = "sigint",   # "sigint" | "clean" | "hard"
    logger: Logger = None
):
    """
    Kill the process when (run, iteration) matches.

    mode:
      - "sigint": simulate Ctrl+C (goes through KeyboardInterrupt)
      - "clean":  sys.exit(0)
      - "hard":   os._exit(1)
    """

    if current_run != target_run:
        return

    if current_iter != target_iter:
        return

    if logger:
        logger.warning(f"stop_if triggered at run {current_run}, iter {current_iter}. Mode: {mode}")
    else:
        print(f"stop_if triggered at run {current_run}, iter {current_iter}. Mode: {mode}")

    if mode == "sigint":
        os.kill(os.getpid(), signal.SIGINT)

    elif mode == "clean":
        sys.exit(0)

    elif mode == "hard":
        os._exit(1)

    else:
        raise ValueError(f"Unknown mode: {mode}")


def attach_debugger():
    # 5678 is the default attach port in the VS Code debug configurations. Unless a host and port are specified, host defaults to localhost.
    debugpy.listen(5678)
    print("Waiting for debugger attach")
    debugpy.wait_for_client()
    print("Debugger attached, starting test...")