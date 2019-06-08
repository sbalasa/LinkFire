import sys
import logging

from .cli import main


logger = logging.getLogger()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Processor exit with: {e}", exc_info=True)
        sys.exit(1)  # exit with error
