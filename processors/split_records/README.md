# split-records

Splits the configured payload array into owned child events. Each child retains the original payload and sets output_field plus parts:{id,index,count}; IDs are deterministic for replay. Empty array emits nothing; cap 4096 parts/1 MiB input. Partial handoff can replay children: downstream deduplication must use child ID. Buffered inputs are rejected; place buffer after split. No automatic child-ACK aggregation is claimed.

The conservative size of all cloned children (input bytes × count) is capped at 8 MiB before allocation.
