"""Compare queue forecasts recorded at collection time with successful finishes."""
import argparse
from contextlib import closing
from datetime import datetime
import json
import os
from pathlib import Path
import sqlite3


def prepare(db):
    version, = db.execute("PRAGMA user_version").fetchone()
    if version == 1:
        return
    if version != 0:
        raise sqlite3.DatabaseError(f"unsupported ETA history version {version}")
    db.execute("""CREATE TABLE runs (run INTEGER PRIMARY KEY, id TEXT, started INTEGER, label TEXT,
                  project TEXT, outcome TEXT, actual INTEGER, UNIQUE(id, started))""")
    db.execute("""CREATE TABLE samples (run INTEGER, issued INTEGER, done INTEGER, total INTEGER,
                  workers INTEGER, eta INTEGER, warning TEXT, PRIMARY KEY (run, issued)) WITHOUT ROWID""")
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='forecasts'").fetchone():
        # Keep the checkpoints from collectors that saved one estimate per quarter.
        db.execute("""INSERT INTO runs(id, started, label, project, outcome, actual)
                      SELECT DISTINCT id, started, label, project, outcome, actual FROM forecasts""")
        db.execute("""INSERT INTO samples SELECT r.run, f.issued, f.done, f.total, f.workers, f.eta, f.warning
                      FROM forecasts f JOIN runs r USING(id, started)""")
        db.execute("DROP TABLE forecasts")
    db.execute("PRAGMA user_version = 1")


def record(path, queues):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.close(os.open(path, os.O_CREAT | os.O_WRONLY, 0o600))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        prepare(db)
        for q in queues:
            key = (q["id"], q["started"])
            state = q["state"]
            if state in ("RUNNING", "DRAINING", "PENDING"):
                # Active rows use the same clock for elapsed time and the ETA.
                issued = q["started"] + q["elapsed"]
                if q["eta"] is not None and 0 < q["done"] < q["total"]:
                    db.execute("INSERT OR IGNORE INTO runs(id, started, label, project) VALUES(?, ?, ?, ?)",
                               (*key, q["label"], q["project"]))
                    db.execute("""INSERT OR IGNORE INTO samples SELECT run, ?, ?, ?, ?, ?, ?
                                  FROM runs WHERE id = ? AND started = ?""",
                               (issued, q["done"], q["total"], q["running"], q["eta"], q.get("eta_warning"), *key))
                db.execute("""UPDATE runs SET outcome = NULL, actual = NULL WHERE id = ? AND started = ?
                              AND outcome IS NOT NULL AND (actual IS NULL OR actual < ?)""", (*key, issued))
            elif state in ("COMPLETED", "FAILED", "KILLED", "FINISHED_INCOMPLETE"):
                end = q["ended"]
                actual = (end if state == "COMPLETED" and q["done"] == q["total"]
                          and end is not None and end > q["started"] else None)
                db.execute("""UPDATE runs SET outcome = ?, actual = ? WHERE id = ? AND started = ?
                              AND (outcome IS NOT ? OR actual IS NOT ?)""", (state, actual, *key, state, actual))


def report(path, search, all_samples=False):
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        # Select checkpoints before sorting; later updates cannot replace the first forecast.
        return [dict(row) for row in db.execute("""
            WITH ordered AS (
                SELECT r.id, r.label, r.project, s.*, r.outcome, r.actual,
                       row_number() OVER (PARTITION BY s.run, (4*s.done / s.total) ORDER BY issued) AS position
                FROM runs r JOIN samples s USING(run) WHERE instr(label, ?) OR instr(id, ?))
            SELECT id, label, project, issued, done, total, workers, eta, warning, issued + eta AS predicted,
                   outcome, actual, CASE WHEN actual >= issued THEN actual - (issued + eta) END AS error_sec
            FROM ordered WHERE ? OR position = 1 ORDER BY issued DESC, id
        """, (search, search, all_samples))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "parallel-workers"
    parser.add_argument("--database", type=Path, default=state / "queue-eta.sqlite3")
    parser.add_argument("--queue", default="", help="substring of the label or queue ID")
    parser.add_argument("--all", action="store_true", help="show every recorded forecast; default: first in each progress quarter")
    parser.add_argument("--json", action="store_true", help="Unix timestamps and signed errors in seconds")
    args = parser.parse_args()
    try:
        rows = report(args.database, args.queue, args.all)
    except sqlite3.Error as error:
        parser.exit(1, f"Cannot read ETA history at {args.database}: {error}\n")
    if args.json:
        print(json.dumps(rows, indent=2))
        return
    def stamp(epoch):
        return datetime.fromtimestamp(epoch).strftime("%m-%d %H:%M") if epoch is not None else "—"
    print("Queue\tJobs when estimated\tWorkers\tForecast made\tPredicted finish\tActual finish\tLate (+) / early (−)\tOutcome")
    for row in rows:
        error = row["error_sec"]
        print(row["label"], f"{row['done']}/{row['total']}", row["workers"] if row["workers"] is not None else "—",
              stamp(row["issued"]), stamp(row["predicted"]),
              stamp(row["actual"]), f"{error/60:+.1f} min" if error is not None else "—",
              row["outcome"] or "awaiting outcome", sep="\t")


if __name__ == "__main__":
    main()
