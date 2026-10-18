# Continue NATSKit — generic configuration-driven publishing

Use https://github.com/majiddarvishan/natskit (\`main\`). Reply in Persian; all repo changes in English. Inspect actual HEAD and read \`.codex/START_HERE.md\`, \`.codex/PUBLISH_SUBJECT_INDEPENDENCE.md\`, \`.codex/TASKS.md\`.

Review committed implementation of \`NK-PUB-01..03\` with Go 1.23.2 and nats.go v1.48.0; run focused disposable tests and \`GOTOOLCHAIN=local task verify:strict\` before changing CRA: a publisher routes to a configured Subject without owning the capturing Stream; preserve real JetStream PubAck, optional expected Stream, independently owned provisioning and old-message safety. Follow with focused disposable tests and \`task verify:strict\` once. Do not introduce CRA-specific flags, mutate runtime/customer brokers, tag/release or change CI; keep \`.codex\` current.

CRA next: https://github.com/Shayan-Pardaz/cra-bridge/blob/feature/natskit-standalone-extraction/.codex/SUBJECT_ROUTING_DESIGN.md
