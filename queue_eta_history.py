"""Record and compare the first displayed ETA in each quarter of a queue's progress."""
import argparse
from contextlib import closing
from datetime import datetime
import json
import os
from pathlib import Path
import sqlite3


def record(path, queues):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # sqlite creates a database with the process umask; monitor files are private.
    os.close(os.open(path, os.O_CREAT | os.O_WRONLY, 0o600))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS forecasts (
            id TEXT, started INTEGER, quarter INTEGER, label TEXT, project TEXT,
            issued INTEGER, done INTEGER, total INTEGER, workers INTEGER, eta INTEGER,
            warning TEXT, outcome TEXT, actual INTEGER, PRIMARY KEY (id, started, quarter))""")
        for q in queues:
            key = (q["id"], q["started"])
            state = q["state"]
            if state in ("RUNNING", "DRAINING", "PENDING"):
                if q["eta"] is not None and 0 < q["done"] < q["total"]:
                    # The monitor computes elapsed and ETA in the same collection.
                    issued = q["started"] + q["elapsed"]
                    db.execute("""INSERT OR IGNORE INTO forecasts VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)""",
                               (*key, 4*q["done"] // q["total"], q["label"], q["project"], issued,
                                q["done"], q["total"], q["running"], q["eta"], q.get("eta_warning")))
                outcome, actual = None, None
            elif state in ("COMPLETED", "FAILED", "KILLED", "FINISHED_INCOMPLETE"):
                outcome = state
                actual = q["ended"] if state == "COMPLETED" and q["done"] == q["total"] else None
            else:
                continue
            db.execute("""UPDATE forecasts SET outcome = ?, actual = ? WHERE id = ? AND started = ?
                          AND (outcome IS NOT ? OR actual IS NOT ?)""",
                       (outcome, actual, *key, outcome, actual))


def report(path, search):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute("""
            SELECT id, label, project, issued, done, total, workers, eta, warning,
                   issued + eta AS predicted, outcome, actual,
                   CASE WHEN actual >= issued THEN actual - (issued + eta) END AS error_sec
            FROM forecasts WHERE instr(label, ?) OR instr(id, ?) ORDER BY issued DESC, id
        """, (search, search))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "parallel-workers"
    parser.add_argument("--database", type=Path, default=state / "queue-eta.sqlite3")
    parser.add_argument("--queue", default="", help="substring of the label or queue ID")
    parser.add_argument("--json", action="store_true", help="Unix timestamps and signed errors in seconds")
    args = parser.parse_args()
    try:
        rows = report(args.database, args.queue)
    except sqlite3.Error as error:
        parser.exit(1, f"Cannot read ETA history at {args.database}: {error}\n")
    if args.json:
        print(json.dumps(rows, indent=2))
        return
    def stamp(epoch):
        return datetime.fromtimestamp(epoch).strftime("%m-%d %H:%M") if epoch is not None else "—"
    print("Queue\tJobs when estimated\tForecast made\tPredicted finish\tActual finish\tLate (+) / early (−)\tOutcome")
    for row in rows:
        error = row["error_sec"]
        print(row["label"], f"{row['done']}/{row['total']}", stamp(row["issued"]), stamp(row["predicted"]),
              stamp(row["actual"]), f"{error/60:+.1f} min" if error is not None else "—",
              row["outcome"] or "awaiting outcome", sep="\t")


if __name__ == "__main__":
    main()
