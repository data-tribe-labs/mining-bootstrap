"""Provision Unity Catalogs for each workshop attendee.

Reads workshop/setup/attendees.yml and, for every attendee:
  1. Creates `{prefix}_{first_name}_{last_name}` catalog if it does not exist.
  2. Sets the attendee as catalog owner.
  3. Grants every OTHER attendee USE CATALOG + USE SCHEMA + SELECT + EXECUTE
     + READ VOLUME on this catalog (so attendees can inspect each other's work
     and compare lineage).

Idempotent — safe to re-run.

Auth: uses the `serverless` profile from ~/.databrickscfg by default.
Override with DATABRICKS_CONFIG_PROFILE.

Usage:
    python workshop/setup/provision_attendees.py
    python workshop/setup/provision_attendees.py --attendees /path/to/attendees.yml
    python workshop/setup/provision_attendees.py --profile serverless
"""

import argparse
import os
import re
import sys
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ATTENDEES_YML = REPO_ROOT / "workshop" / "setup" / "attendees.yml"
DEFAULT_OUTPUT_MD = REPO_ROOT / "workshop" / "setup" / "attendees_provisioned.md"


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def catalog_name(prefix: str, attendee: dict) -> str:
    return f"{prefix}_{slugify(attendee['first_name'])}_{slugify(attendee['last_name'])}"


def load_attendees(path: Path) -> tuple[str, list[dict]]:
    with path.open() as f:
        data = yaml.safe_load(f)
    prefix = data["prefix"]
    attendees = data["attendees"]
    for i, att in enumerate(attendees):
        for field in ("first_name", "last_name", "email"):
            if not att.get(field) or att[field].startswith("CHANGE_ME"):
                sys.exit(f"attendees.yml entry #{i + 1} is missing or unedited '{field}'")
    return prefix, attendees


def run_sql(w: WorkspaceClient, warehouse_id: str | None, statement: str) -> None:
    """Execute a SQL statement. Uses the first running warehouse if none specified."""
    if warehouse_id is None:
        warehouses = list(w.warehouses.list())
        if not warehouses:
            sys.exit("No SQL warehouses found — cannot execute grant statements")
        warehouse_id = warehouses[0].id
    resp = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    if resp.status and resp.status.state and resp.status.state.value not in ("SUCCEEDED",):
        err = resp.status.error.message if resp.status.error else "unknown error"
        raise RuntimeError(f"SQL failed: {statement}\n  -> {err}")


def provision(prefix: str, attendees: list[dict], profile: str | None, warehouse_id: str | None) -> None:
    if profile:
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
    w = WorkspaceClient()

    catalogs = [(att, catalog_name(prefix, att)) for att in attendees]

    print(f"Provisioning {len(catalogs)} catalogs on {w.config.host}")
    print()

    for att, cat in catalogs:
        print(f"  {cat}  →  {att['email']}")
    print()

    for att, cat in catalogs:
        print(f"[{cat}] creating catalog")
        run_sql(w, warehouse_id, f"CREATE CATALOG IF NOT EXISTS `{cat}`")
        run_sql(w, warehouse_id, f"ALTER CATALOG `{cat}` OWNER TO `{att['email']}`")

    print()
    print("Cross-granting read access between attendees…")
    for owner_att, owner_cat in catalogs:
        for other_att, _ in catalogs:
            if other_att["email"] == owner_att["email"]:
                continue
            run_sql(
                w,
                warehouse_id,
                f"GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME "
                f"ON CATALOG `{owner_cat}` TO `{other_att['email']}`",
            )

    write_summary(catalogs, DEFAULT_OUTPUT_MD)
    print(f"\nDone. Summary written to {DEFAULT_OUTPUT_MD}")


def write_summary(catalogs: list[tuple[dict, str]], out_path: Path) -> None:
    lines = ["# Worley Workshop — Attendee Catalogs", ""]
    lines.append("| Attendee | Email | Catalog |")
    lines.append("|---|---|---|")
    for att, cat in catalogs:
        name = f"{att['first_name']} {att['last_name']}"
        lines.append(f"| {name} | {att['email']} | `{cat}` |")
    lines.append("")
    lines.append("All catalogs: owner = the attendee, read access = all other attendees.")
    out_path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--attendees", type=Path, default=DEFAULT_ATTENDEES_YML)
    parser.add_argument("--profile", default="serverless", help="databricks-cli profile (default: serverless)")
    parser.add_argument("--warehouse-id", help="SQL warehouse ID to run grants on (default: first available)")
    args = parser.parse_args()

    prefix, attendees = load_attendees(args.attendees)
    provision(prefix, attendees, args.profile, args.warehouse_id)


if __name__ == "__main__":
    main()
