"""Drop every attendee catalog created by provision_attendees.py.

Destructive — requires --confirm to run. Use after the workshop for cleanup.

Usage:
    python workshop/setup/deprovision_attendees.py --confirm
    python workshop/setup/deprovision_attendees.py --confirm --attendees /path/to/attendees.yml
"""

import argparse
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from provision_attendees import (
    DEFAULT_ATTENDEES_YML,
    catalog_name,
    load_attendees,
    run_sql,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--attendees", type=Path, default=DEFAULT_ATTENDEES_YML)
    parser.add_argument("--profile", default="serverless")
    parser.add_argument("--warehouse-id")
    parser.add_argument("--confirm", action="store_true", help="Required — deprovision is destructive")
    args = parser.parse_args()

    if not args.confirm:
        sys.exit("Refusing to drop catalogs without --confirm")

    if args.profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = args.profile
    w = WorkspaceClient()

    prefix, attendees = load_attendees(args.attendees)
    catalogs = [catalog_name(prefix, att) for att in attendees]

    print(f"About to DROP {len(catalogs)} catalogs on {w.config.host}:")
    for cat in catalogs:
        print(f"  - {cat}")
    print()

    for cat in catalogs:
        print(f"[{cat}] dropping catalog CASCADE")
        run_sql(w, args.warehouse_id, f"DROP CATALOG IF EXISTS `{cat}` CASCADE")

    print("\nDone.")


if __name__ == "__main__":
    main()
