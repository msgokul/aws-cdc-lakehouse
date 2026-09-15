"""Create, run, and DEBUG the Glue Data Quality ruleset from the CLI.

Console-free path for Day 5 step 3 (the console buries Data Quality under
Glue -> Data Catalog -> Tables -> <table> -> Data quality tab).

Run in CloudShell, from the folder holding maple_fact_quality.dqdl:

    python3 dq_ruleset.py test      # submit each rule ALONE -> find the bad one
    python3 dq_ruleset.py create    # create (or update) the full ruleset
    python3 dq_ruleset.py run       # evaluate it, poll, print per-rule outcomes
    python3 dq_ruleset.py show      # print the stored DQDL
    python3 dq_ruleset.py delete    # remove it

`test` is the one to reach for when the API says "DataQuality rules cannot be
parsed": it isolates the offending rule instead of leaving you bisecting by
hand. It uses a scratch ruleset name and cleans up after itself.

Everything sent to the API is normalised first: CRLF -> LF (Windows files
uploaded to CloudShell carry \\r, which the DQDL lexer rejects) and the whole
ruleset is flattened to a single line, so indentation can never be the
problem.

> 📝 EXAM: `run` performs exactly the three calls the Step Functions workflow
  makes - StartDataQualityRulesetEvaluationRun -> (poll)
  GetDataQualityRulesetEvaluationRun -> GetDataQualityResult. There is no
  `.sync` integration for DQ, which is *why* the state machine needs a
  Wait/Choice polling loop.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import boto3

REGION = "ca-central-1"
RULESET = "maple-fact-quality"
SCRATCH = "maple-dq-scratch"
DATABASE = "maple_curated"
TABLE = "fact_order_item"
GLUE_ROLE = "maple-glue-role"
DQDL_FILE = "maple_fact_quality.dqdl"

glue = boto3.client("glue", region_name=REGION)
TARGET = {"TableName": TABLE, "DatabaseName": DATABASE}


# --------------------------------------------------------------------------
# Parsing the local file
# --------------------------------------------------------------------------
def read_rules_block() -> str:
    path = Path(DQDL_FILE)
    if not path.exists():
        sys.exit(f"{DQDL_FILE} not found - upload it to CloudShell first "
                 "(Actions -> Upload file).")
    text = path.read_text(encoding="utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
    # "Rules" alone also matches the word "Ruleset" in the documentation header.
    match = re.search(r"^\s*Rules\s*=\s*\[", text, flags=re.MULTILINE)
    if not match:
        sys.exit("No 'Rules = [' block found in the DQDL file.")
    block = text[match.start():].strip()
    if "/*" in block or "*/" in block:
        sys.exit("The Rules block contains a /* comment */ - DQDL rejects "
                 "comments inside it. Move them above 'Rules = ['.")
    return block


def split_rules(block: str) -> list[str]:
    """Split the block into individual rules, respecting quotes and brackets
    so that commas inside "in [...]" lists or SQL strings don't split a rule."""
    inner = block[block.index("[") + 1 : block.rindex("]")]
    rules, current, depth, in_quotes = [], [], 0, False
    for ch in inner:
        if ch == '"':
            in_quotes = not in_quotes
        if not in_quotes:
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
            elif ch == "," and depth == 0:
                rules.append("".join(current).strip())
                current = []
                continue
        current.append(ch)
    if "".join(current).strip():
        rules.append("".join(current).strip())
    return [" ".join(r.split()) for r in rules if r.strip()]


def as_one_line(rules: list[str]) -> str:
    return "Rules = [ " + ", ".join(rules) + " ]"


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def put_ruleset(name: str, rules_text: str) -> None:
    try:
        glue.create_data_quality_ruleset(
            Name=name,
            Description="Maple & Co. curated fact table quality gate",
            Ruleset=rules_text,
            TargetTable=TARGET,
        )
    except glue.exceptions.AlreadyExistsException:
        glue.update_data_quality_ruleset(Name=name, Ruleset=rules_text)


def cmd_test() -> None:
    """Submit each rule on its own; report which ones the parser rejects."""
    rules = split_rules(read_rules_block())
    print(f"Testing {len(rules)} rule(s) individually against the DQDL parser.\n")

    good, bad = [], []
    for rule in rules:
        try:
            put_ruleset(SCRATCH, f"Rules = [ {rule} ]")
            print(f"  OK    {rule[:88]}")
            good.append(rule)
        except glue.exceptions.InvalidInputException as exc:
            print(f"  PARSE FAIL  {rule[:80]}")
            print(f"      -> {exc.response['Error']['Message']}")
            bad.append(rule)
        except Exception as exc:  # permissions, throttling, etc.
            print(f"  ERROR {rule[:70]} -> {type(exc).__name__}: {exc}")
            bad.append(rule)

    try:
        glue.delete_data_quality_ruleset(Name=SCRATCH)
    except Exception:
        pass

    print(f"\n{len(good)} rule(s) parse, {len(bad)} rejected.")
    if bad:
        print("\nDrop or rewrite the rejected rule(s). The rest, as one line:\n")
        print(as_one_line(good))
    else:
        print("All rules parse individually - the full block should save. "
              "Run: python3 dq_ruleset.py create")


def cmd_create(inline: str | None = None) -> None:
    rules = split_rules(read_rules_block()) if inline is None else split_rules(inline)
    text = as_one_line(rules)
    print(f"Submitting {len(rules)} rule(s):\n{text}\n")
    try:
        put_ruleset(RULESET, text)
    except glue.exceptions.InvalidInputException as exc:
        sys.exit(f"Parser rejected the ruleset: "
                 f"{exc.response['Error']['Message']}\n"
                 "Run 'python3 dq_ruleset.py test' to find which rule.")
    print(f"Ruleset '{RULESET}' saved against {DATABASE}.{TABLE}.")


def cmd_show() -> None:
    r = glue.get_data_quality_ruleset(Name=RULESET)
    print(f"Ruleset: {r['Name']}  target: {r['TargetTable']['DatabaseName']}."
          f"{r['TargetTable']['TableName']}\n")
    print(r["Ruleset"])


def cmd_delete() -> None:
    glue.delete_data_quality_ruleset(Name=RULESET)
    print(f"Deleted ruleset '{RULESET}'.")


def cmd_run() -> None:
    run_id = glue.start_data_quality_ruleset_evaluation_run(
        DataSource={"GlueTable": {"DatabaseName": DATABASE, "TableName": TABLE}},
        Role=GLUE_ROLE,
        NumberOfWorkers=2,
        Timeout=20,
        RulesetNames=[RULESET],
    )["RunId"]
    print(f"Started evaluation run {run_id}")

    while True:
        run = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        status = run["Status"]
        print(f"  status: {status}")
        if status not in ("STARTING", "RUNNING"):
            break
        time.sleep(20)

    if status != "SUCCEEDED":
        sys.exit(f"Evaluation run did not succeed: {run.get('ErrorString', status)}")

    result = glue.get_data_quality_result(ResultId=run["ResultIds"][0])
    score = result.get("Score", 0.0)

    print(f"\n{'rule':<70} {'outcome':<8}")
    print("-" * 80)
    failures = 0
    for rr in result["RuleResults"]:
        outcome = rr.get("Result", "?")
        if outcome != "PASSED":
            failures += 1
        label = rr.get("Description") or rr.get("Name", "")
        print(f"{label[:69]:<70} {outcome:<8}")
        if outcome != "PASSED" and rr.get("EvaluationMessage"):
            print(f"    -> {rr['EvaluationMessage']}")

    print("-" * 80)
    print(f"SCORE: {score:.2f}   failed rules: {failures}")
    if score < 1.0:
        print("\nThis is what the Step Functions QualityGate blocks on: a score "
              "below 1.0 stops the pipeline and alerts instead of publishing "
              "bad data.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["test", "create", "run", "show", "delete"])
    p.add_argument("--inline", help="use this Rules = [...] string instead of the file")
    args = p.parse_args()

    if args.command == "test":
        cmd_test()
    elif args.command == "create":
        cmd_create(args.inline)
    elif args.command == "run":
        cmd_run()
    elif args.command == "show":
        cmd_show()
    else:
        cmd_delete()
