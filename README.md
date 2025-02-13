This repository contains all the SMTs (Single Message Transformers) we use as part of our microservice data
synchronization.

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

## Join fields

Kafka Connect SMT to join different values into a new one using a separator

This SMT supports joining values into the record Key or Value

Properties:

| Name                      | Description                                                                                                                    | Type   | Default    | Importance |
|---------------------------|--------------------------------------------------------------------------------------------------------------------------------|--------|------------|------------|
| `join-fields.fields`      | List of the fields keys to join into the destination key                                                                       | List   | empty list | High       |
| `join-fields.separator`   | Separator of the fields values joined                                                                                          | String | .          | High       |
| `join-fields.destination` | Key name of the joined field (with ? suffix to make it optional or with ! to make it mandatory, will be optional if no suffix) | String | output     | High       |

Example on how to add to your connector:

```
transforms="generateTableFullName"
transforms.generateTableFullName.type="com.jobteaser.kafka.connect.transforms.JoinFields$Value"
transforms.generateTableFullName.join-fields.fields="__source_db,__source_table"
transforms.generateTableFullName.join-fields.separator="."
transforms.generateTableFullName.join-fields.destination="fully_qualified_name"
```

Before:

```json
{
  "__source_db": "my_database",
  "__source_table": "my_table",
  "id": 123
}
```

After:

```json
{
  "__source_db": "my_database",
  "__source_table": "my_table",
  "fully_qualified_name": "my_database.my_table",
  "id": 123
}
```

## Change case

Kafka Connect SMT to change case of a field value.

This SMT supports change case on the record Key or Value.

Properties:

| Name                     | Description                                                                                    | Type   | Default      | Importance |
|--------------------------|------------------------------------------------------------------------------------------------|--------|--------------|------------|
| `change-case.field-name` | Name of the field we want to change case                                                       | String | empty string | High       |
| `change-case.case`       | Wanted case (can be lowercase or uppercase). If it is not recognized, case will not be changed | String | empty string | High       |

Example on how to add to your connector:

```
transforms="changeCase"
transforms.changeCase.type="com.jobteaser.kafka.connect.transforms.ChangeCase$Value"
transforms.changeCase.change-case.field-name="my_field"
transforms.changeCase.change-case.case="lowercase"
```

Before:

```json
{
  "my_field": "Field value",
  "id": 123
}
```

After:

```json
{
  "my_field": "field value",
  "id": 123
}
```
