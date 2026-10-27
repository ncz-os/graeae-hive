# Deployed Hive worker

`hive_worker.py` is the tracked copy of the executable deployed as
`/opt/graeae/hive_worker.py`. The repository does not contain an installation
procedure or all of its deployed Python sidecars (`hive_budget`, `provenance`,
and several `hive` modules). Before installation, identify the host's existing
service and install the worker together with its matching dependencies using the
fleet's established deployment procedure; this checkout alone is not a complete
runtime. No installation or service restart was performed here.

Keep its behavior aligned with `hive/workers/run.py`; document any intentional
divergence and its reason when changing either worker.
