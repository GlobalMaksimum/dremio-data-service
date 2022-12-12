# Data Services using Dremio

## Data Service

Bulk data provider using user defined or stock SQL statements. Some challanges

- Security
- Performance of JSON serialization
- Blocking execution
- No catalog of data (Governence)

## Advantages

1. Async execution and result set saving prevents from multiple execution or cursor state management.
2. gRPC based arrow based engine is way faster in large results
3. Tags & Wiki allows end user programs to access metadata information
4. **[Enterprise Only]* You can expose dependency between `source -> dataset -> virtual dataset -> virtual dataset`
5. Authentication & Authorization is the same with Dremio UI.