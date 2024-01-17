This repository contains all the SMTs (Single Message Transformers) we use as part of our microservice data synchronization.

## Email obfuscation

Kafka Connect SMT to obfuscate an existing email field.

This SMT supports obfuscating an email into the record Key or Value.

Properties:

| Name               | Description             | Type   | Default | Importance |
|--------------------|-------------------------|--------|---------|------------|
| `email.field.name` | Field name to obfuscate | String | `email` | High       |


Example on how to add to your connector:
```
transforms="obfuscate-email"
transforms.obfuscate-email.type="com.jobteaser.kafka.connect.transforms.ObfuscateEmail$Value"
transforms.obfuscate-email.email.field.name="email"
```
