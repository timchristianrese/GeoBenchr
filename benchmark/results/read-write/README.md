# Read-write workload

No read-write workload has been measured yet. The benchmark defines write queries (inserts of new points and
trajectories per scenario), but they are switched off in every run so far: every run reads the same, unchanged data,
and runs with different numbers of client threads stay comparable.

The read-only results are in [`../read-only/`](../README.md#read-only).
