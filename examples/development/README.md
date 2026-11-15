# Development configuration examples

These JSON files are Connector resource and node configuration fragments, not complete rule-import documents. Bind node connector_ref to the corresponding resource. Loopback endpoints require your simulator; no example should be pointed at a production actuator without a separate reviewed test plan.

Modbus: source-modbus -> normalize-points -> condition-router (edges labelled matched/otherwise) -> buffer-node -> existing sink-mqtt. MC uses the same node pattern with source-mc. New Source telemetry includes quality; use transform-map when an external endpoint requires only device_code/data/time. Rate-limit and window-aggregate intentionally change or reduce events and belong before buffering.

OPC UA local-test config explicitly uses anonymous None security for a loopback simulator. Configure SignAndEncrypt plus client_certificate_file/private_key_file/server_certificate_file for production. Actual NodeIds must be verified on your server.

SQL Server examples use a reserved hostname and omit credentials: configure them through protected Connector settings. The sample sink SQL assumes a unique event_id key and uses transaction-held key-range locking for idempotent replay. Create and validate the schema and database permissions separately. Driver-fixture tests do not prove these statements on your server version.

See each PluginInfo.ConfigSchema and the implementation-status document for supported modes and remaining work.
