# cl_snapshot
Chandy-Lamport Algorithm try in Python. 

Tested and working script to emulate Chandy-Lamport algorithm to capture snapshots on distributed systems, as part of University course lab exercise. The script can work on any number of processes/machines and any process/machine can initiate Snapshot(s), with a minor edit. For simplicity sake the default script only lets one process/machine to initiate with a conditional check. If you need to test it, make sure to update the "CLIENT_LIST" to suit your environment.

Series of global snapshots are possible and all of them are saved with timestamps.Vector-Clock method is used to pass on states between events. Messages being sent can be modified to suit.

Only external library being used is "PTable" to print out tabular data of current statuses.
