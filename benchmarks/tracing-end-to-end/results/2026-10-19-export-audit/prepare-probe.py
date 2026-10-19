"""Prepare the retained isolated Cargo diagnostic without changing registry data."""
from pathlib import Path
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path)
parser.add_argument("--phases", action="store_true")
args = parser.parse_args()
report = Path(__file__).resolve().parent
repo = report.parents[3]
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=False)
(output / "core").mkdir()
(output / "src").mkdir()
old_repo = "/home/dtietjen/fast-telemetry-eden-2515"
core = (report / "measured-core.Cargo.toml").read_text().replace(old_repo, str(repo))
manifest = (report / "measured.Cargo.toml").read_text().replace(
    '../../batch-knee-20261019/core-tests', 'core'
).replace('[workspace]', '[workspace]\nexclude = ["core"]')
(output / "core/Cargo.toml").write_text(core)
(output / "Cargo.toml").write_text(manifest)
(output / "Cargo.lock").write_bytes((report / "measured.Cargo.lock").read_bytes())
source = "encode-probe-v2.rs" if args.phases else "encode-probe-v1.rs"
(output / "src/main.rs").write_bytes((report / source).read_bytes())
print(output / "Cargo.toml")
