"""Console script for bqrun."""
import sys
from bqrun import bqrun


def main():
    bqrun.setup_logging()
    parser = bqrun.setup_parser()
    args = parser.parse_args()

    bqrun.main(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
