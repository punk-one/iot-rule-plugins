# Buffer fault checks

Default native tests include a real child process that commits a queue record, is forcibly killed, then reopens/replays the same event identity. `scripts/test-buffer-faults.sh` additionally requires Linux user/mount namespaces and creates a private 8 MiB tmpfs. It fills only that mount, verifies a failed transaction is not falsely acknowledged and does not change prior usage/records, frees space, and verifies recovery. No actual workspace/system disk is filled and no host service is installed.

These tests cover software SIGKILL/disk-full behavior, not power-loss storage certification. The two Engine receipt integration adds broker receipt vs database commit separation, lost/forged receipt handling and duplicate-business-row prevention. Hardware, extended offline periods, all broker versions and 72h acceptance remain production gates.
