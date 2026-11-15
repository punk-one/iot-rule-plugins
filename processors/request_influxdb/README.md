# request-influxdb

Flux query and payload path parameters are sent separately. Output is a clone of the input with query_result.rows (text cells), row_count and status. Use params in trusted Flux code rather than building query strings. No implicit timestamp or number coercion.
