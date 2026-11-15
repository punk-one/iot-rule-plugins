# render-template

Interpolate {{payload.path}} or {{event.id}} into a cloned event output_field. HTML escaping is default; JSON inserts a fully quoted/encoded JSON value (do not add extra quotes in the template); text explicitly disables escaping. Missing paths fail. Bounded 256 placeholders, 64 KiB template, 1 MiB output. This deliberately small syntax has no loops, helpers, file access, functions, npm or JSONata compatibility claim.
