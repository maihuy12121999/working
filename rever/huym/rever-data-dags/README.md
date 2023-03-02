# Default config

### ForceRun

There are 3 params in order to force run a DAG.

- `forcerun.enabled`: True if we want to force run this DAG.
- `forcerun.from_date` & `forcerun.to_date`: (yyyy-MM-dd) the date range to force run this DAG, inclusive

E.g: The below setting is used to force run a DAG from 01 Jan 2022 to 05 Jan 2022

```json
{
  "forcerun.enabled": "true",
  "forcerun.from_date": "2022-01-01",
  "forcerun.to_date": "2022-01-05"
}
```

### Data Sync All

For DAG tagged with `Data Sync` only. If we want to sync all data:

```json
{
  "is_sync_all": "true"
}
```