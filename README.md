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
transforms=obfuscateemail
transforms.obfuscateemail.type=com.jobteaser.kafka.connect.smt.ObfuscateEmail$Value
transforms.obfuscateemail.email.field.name="email"
```
