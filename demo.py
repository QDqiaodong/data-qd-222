from pathlib import Path
from tempfile import TemporaryDirectory
from distribution import ConflictingPromotion, Distribution

with TemporaryDirectory() as root:
    path = Path(root, "config.db")
    publisher = Distribution(path)
    subscriber = Distribution(path)
    stable = publisher.stage({"upstream": "stable", "timeout_ms": 60})
    candidate = publisher.stage({"upstream": "candidate", "timeout_ms": 70})
    publisher.promote(stable, expected_generation=0)
    print(subscriber.poll())
    try:
        publisher.promote(candidate, expected_generation=0)
    except ConflictingPromotion:
        print("stale write rejected")
    publisher.promote(candidate, expected_generation=1)
    print(subscriber.poll(1))
    publisher.promote(stable, expected_generation=2)
    print(subscriber.poll(2))

    # Retire the now-unused candidate, hold a copy on the duty pin, then collect.
    pin_id = publisher.pin_revisions((candidate,))
    publisher.retire_revisions((candidate,))
    print("retained while pinned:", publisher.collect_retired())
    print("pin read-back:", publisher.read_pin(pin_id))
    publisher.unpin_revisions(pin_id)
    print("collected after release:", publisher.collect_retired())
