"""Configure Lake Formation for the analyst / fraud split — entirely via API.

The console's principal picker paginates ListRoles and frequently fails to
find a newly created role. This does the same work deterministically, and
leaves the grants visible in code review rather than buried in a wizard.

Run in CloudShell (as your data lake admin user):

    python3 lakeformation_setup.py status      # what exists / what is granted
    python3 lakeformation_setup.py setup       # register, revoke legacy, filter, grant
    python3 lakeformation_setup.py teardown    # remove the filter and grants

Prerequisites: maple-analyst-role and maple-fraud-role exist (Day 6 step 2c),
maple_curated.customer_pii is registered, and your user is a Lake Formation
data lake administrator.

> 📝 EXAM: the three moving parts, in the order they must happen —
  1. REGISTER the S3 location with LF (LF then vends S3 credentials at query
     time; this is why the roles need no s3:GetObject on the data),
  2. REVOKE the legacy IAMAllowedPrincipals grant (otherwise LF permissions
     are advisory and everyone still sees everything),
  3. GRANT: a data cells filter (columns + row expression) to the restricted
     principal, and plain table SELECT to the privileged one.
"""

from __future__ import annotations

import sys

import boto3

REGION = "ca-central-1"
DATABASE = "maple_curated"
PII_TABLE = "customer_pii"
FILTER_NAME = "analyst_no_pii"

ANALYST_ROLE = "maple-analyst-role"
FRAUD_ROLE = "maple-fraud-role"

# Columns the analyst must never see, and the rows they may see.
PII_COLUMNS = ["full_name", "email", "phone", "sin_test"]
ROW_FILTER = "province = 'ON'"

# Tables both roles need for the join queries.
SHARED_TABLES = ["fact_order_item", "dim_customer", "dim_product", "dim_date"]

lf = boto3.client("lakeformation", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
iam = boto3.client("iam")
sts = boto3.client("sts")

ACCOUNT = sts.get_caller_identity()["Account"]


def role_arn(name: str) -> str:
    return f"arn:aws:iam::{ACCOUNT}:role/{name}"


def require_roles() -> None:
    missing = []
    for name in (ANALYST_ROLE, FRAUD_ROLE):
        try:
            iam.get_role(RoleName=name)
        except iam.exceptions.NoSuchEntityException:
            missing.append(name)
    if missing:
        existing = [r["RoleName"] for r in iam.list_roles()["Roles"]
                    if r["RoleName"].startswith("maple")]
        sys.exit(f"Missing role(s): {', '.join(missing)}\n"
                 f"Roles starting with 'maple' that DO exist: {existing}\n"
                 "Create them (Day 6 step 2c) or fix the names at the top of this file.")


def register_location(bucket: str, prefix: str) -> None:
    path = f"arn:aws:s3:::{bucket}/{prefix}"
    try:
        lf.register_resource(ResourceArn=path, UseServiceLinkedRole=True)
        print(f"  registered {path}")
    except lf.exceptions.AlreadyExistsException:
        print(f"  already registered: {path}")


def revoke_legacy_grant() -> None:
    """Remove IAMAllowedPrincipals so LF permissions actually bite."""
    for resource, label in (
        ({"Table": {"DatabaseName": DATABASE, "Name": PII_TABLE}}, f"table {PII_TABLE}"),
        ({"Database": {"Name": DATABASE}}, f"database {DATABASE}"),
    ):
        try:
            lf.revoke_permissions(
                Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                Resource=resource,
                Permissions=["ALL"],
            )
            print(f"  revoked IAMAllowedPrincipals on {label}")
        except Exception as exc:
            # Not granted in the first place is the good outcome.
            print(f"  IAMAllowedPrincipals on {label}: nothing to revoke ({type(exc).__name__})")


def create_filter() -> None:
    try:
        lf.create_data_cells_filter(
            TableData={
                "TableCatalogId": ACCOUNT,
                "DatabaseName": DATABASE,
                "TableName": PII_TABLE,
                "Name": FILTER_NAME,
                "RowFilter": {"FilterExpression": ROW_FILTER},
                "ColumnWildcard": {"ExcludedColumnNames": PII_COLUMNS},
            }
        )
        print(f"  created data cells filter '{FILTER_NAME}' "
              f"(excludes {', '.join(PII_COLUMNS)}; rows where {ROW_FILTER})")
    except lf.exceptions.AlreadyExistsException:
        print(f"  filter '{FILTER_NAME}' already exists")


def grant(principal: str, resource: dict, permissions: list[str], label: str) -> None:
    lf.grant_permissions(
        Principal={"DataLakePrincipalIdentifier": principal},
        Resource=resource,
        Permissions=permissions,
    )
    print(f"  granted {'+'.join(permissions)} on {label}")


def cmd_setup(bucket: str, prefix: str) -> None:
    require_roles()

    print("1. Registering the data location with Lake Formation")
    register_location(bucket, prefix)

    print("\n2. Removing the legacy IAMAllowedPrincipals grant")
    revoke_legacy_grant()

    print("\n3. Creating the analyst data cells filter")
    create_filter()

    print("\n4. Granting")
    # Analyst: filtered access only - no columns, no rows beyond the filter.
    grant(role_arn(ANALYST_ROLE),
          {"DataCellsFilter": {"TableCatalogId": ACCOUNT, "DatabaseName": DATABASE,
                               "TableName": PII_TABLE, "Name": FILTER_NAME}},
          ["SELECT"],
          f"{ANALYST_ROLE} -> {PII_TABLE} via {FILTER_NAME}")

    # Fraud: the whole table.
    grant(role_arn(FRAUD_ROLE),
          {"Table": {"CatalogId": ACCOUNT, "DatabaseName": DATABASE, "Name": PII_TABLE}},
          ["SELECT"],
          f"{FRAUD_ROLE} -> {PII_TABLE} (all columns, all rows)")

    # Both roles need the star schema for the join queries.
    for role in (ANALYST_ROLE, FRAUD_ROLE):
        grant(role_arn(role), {"Database": {"Name": DATABASE}}, ["DESCRIBE"],
              f"{role} -> database {DATABASE}")
        for table in SHARED_TABLES:
            try:
                grant(role_arn(role),
                      {"Table": {"CatalogId": ACCOUNT, "DatabaseName": DATABASE, "Name": table}},
                      ["SELECT"], f"{role} -> {table}")
            except Exception as exc:
                print(f"  ! {role} -> {table}: {type(exc).__name__} ({exc})")

    print("\nDone. Prove it:  python3 assume_and_query.py --compare")


def cmd_status() -> None:
    print(f"Account {ACCOUNT}, region {REGION}\n")

    print("Roles starting with 'maple':")
    for r in iam.list_roles()["Roles"]:
        if r["RoleName"].startswith("maple"):
            print(f"  {r['RoleName']}")

    print(f"\nTables in {DATABASE}:")
    for t in glue.get_tables(DatabaseName=DATABASE)["TableList"]:
        print(f"  {t['Name']}")

    print("\nRegistered Lake Formation locations:")
    for r in lf.list_resources().get("ResourceInfoList", []):
        print(f"  {r['ResourceArn']}")

    print(f"\nData cells filters on {PII_TABLE}:")
    filters = lf.list_data_cells_filter(
        Table={"CatalogId": ACCOUNT, "DatabaseName": DATABASE, "Name": PII_TABLE}
    ).get("DataCellsFilters", [])
    for f in filters:
        excluded = f.get("ColumnWildcard", {}).get("ExcludedColumnNames", [])
        print(f"  {f['Name']}: excludes {excluded}, rows: "
              f"{f.get('RowFilter', {}).get('FilterExpression', 'all')}")
    if not filters:
        print("  (none)")

    print(f"\nPermissions on {DATABASE}:")
    perms = lf.list_permissions(
        Resource={"Database": {"Name": DATABASE}}
    ).get("PrincipalResourcePermissions", [])
    for p in perms:
        who = p["Principal"]["DataLakePrincipalIdentifier"].split("/")[-1]
        print(f"  {who:<28} {p['Permissions']}")

    print(f"\nPermissions on {PII_TABLE}:")
    for p in lf.list_permissions(
        Resource={"Table": {"CatalogId": ACCOUNT, "DatabaseName": DATABASE, "Name": PII_TABLE}}
    ).get("PrincipalResourcePermissions", []):
        who = p["Principal"]["DataLakePrincipalIdentifier"].split("/")[-1]
        print(f"  {who:<28} {p['Permissions']}")
        if who == "IAM_ALLOWED_PRINCIPALS":
            print("      ^^ THIS MAKES LF PERMISSIONS ADVISORY. Run 'setup' to revoke it.")


def cmd_teardown() -> None:
    try:
        lf.delete_data_cells_filter(
            TableCatalogId=ACCOUNT, DatabaseName=DATABASE,
            TableName=PII_TABLE, Name=FILTER_NAME)
        print(f"deleted filter {FILTER_NAME}")
    except Exception as exc:
        print(f"filter: {type(exc).__name__}")
    for role in (ANALYST_ROLE, FRAUD_ROLE):
        try:
            lf.revoke_permissions(
                Principal={"DataLakePrincipalIdentifier": role_arn(role)},
                Resource={"Table": {"CatalogId": ACCOUNT, "DatabaseName": DATABASE,
                                    "Name": PII_TABLE}},
                Permissions=["SELECT"])
            print(f"revoked {role} on {PII_TABLE}")
        except Exception as exc:
            print(f"{role}: {type(exc).__name__}")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["setup", "status", "teardown"])
    p.add_argument("--bucket", default="maple-sprint-curated")
    p.add_argument("--prefix", default="pii/")
    a = p.parse_args()

    if a.command == "setup":
        cmd_setup(a.bucket, a.prefix)
    elif a.command == "status":
        cmd_status()
    else:
        cmd_teardown()
