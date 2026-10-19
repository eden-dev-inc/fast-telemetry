"""Keep benchmark reproduction above the disk floor and honor host admission."""
from pathlib import Path
import datetime
import json
import shutil


def gate(output, name):
    available = shutil.disk_usage(output).free
    if available < 20_000_000_000:
        raise RuntimeError("benchmark needs at least 20 GB free")
    guard = Path.home() / ".local/state/eden-resource-guard"
    if (guard / "DO_NOT_START_NEW_ADAM_RUNS").exists():
        raise RuntimeError("host resource guard closed")
    status_file = guard / "disk-status.tsv"
    receipt = {"live_available_bytes": available}
    if status_file.exists():
        stamp, free, threshold, status = status_file.read_text().splitlines()[1].split()
        age = (datetime.datetime.now(datetime.timezone.utc)
               - datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds()
        if not (0 <= age <= 90 and status == "OK"
                and int(free) >= int(threshold) >= 20_000_000_000):
            raise RuntimeError("host resource guard closed or stale")
        receipt.update(guard_age_seconds=age, guard_available_bytes=int(free), status=status)
    (output / (name + ".guard.json")).write_text(json.dumps(receipt, indent=2) + "\n")
