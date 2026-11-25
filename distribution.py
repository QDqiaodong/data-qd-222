"""Versioned configuration consumed by service instances through one active pointer."""
from contextlib import closing
import json
import sqlite3


class ConflictingPromotion(Exception):
    pass


class Distribution:
    def __init__(self, path):
        self.path = path
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript("""CREATE TABLE IF NOT EXISTS revision (
                id INTEGER PRIMARY KEY, config TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS channel (
                slot INTEGER PRIMARY KEY CHECK (slot=1), revision INTEGER, generation INTEGER NOT NULL);
                INSERT OR IGNORE INTO channel VALUES (1, NULL, 0);""")

    def stage(self, config):
        if (type(config) is not dict or set(config) != {"upstream", "timeout_ms"}
                or not isinstance(config["upstream"], str) or not config["upstream"].strip()
                or type(config["timeout_ms"]) is not int or config["timeout_ms"] <= 0):
            raise ValueError("configuration requires upstream and a positive integer timeout_ms")
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute("INSERT INTO revision(config) VALUES (?)",
                              (json.dumps(config, sort_keys=True),)).lastrowid

    def promote(self, revision, *, expected_generation):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM revision WHERE id=?", (revision,)).fetchone():
                raise KeyError(revision)
            if db.execute("""UPDATE channel SET revision=?, generation=generation+1
                             WHERE slot=1 AND generation=?""",
                          (revision, expected_generation)).rowcount != 1:
                raise ConflictingPromotion(expected_generation)
            return expected_generation + 1

    def poll(self, known_generation=None):
        with closing(sqlite3.connect(self.path)) as db:
            generation, revision, data = db.execute("""
                SELECT c.generation,c.revision,r.config FROM channel c
                LEFT JOIN revision r ON r.id=c.revision WHERE c.slot=1""").fetchone()
        if revision is None or generation == known_generation:
            return None
        return generation, revision, json.loads(data)
