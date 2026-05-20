# ToDo List

- Finish adding new scraper test cases including fixtures and cassettes
- Remove old scraper test cases and any unused code
- Maybe then move on to integration tests?





Next Steps
- Contracts - bronze layer, silver dim_injuries scd2
- Unit Tests - Idempotency (bronze layer, silver dim_injuries scd2)
- Notebook - Entity Resolution
- Unit Tests - Entity Resolution

Later?
- Research - Scraper test approach & follow-on tasks like designing and writing test cases
- Contracts (Data Modelling) - rest of silver layer
- Unit Tests - Idempotency (rest of silver layer)
- Contracts (Data Modelling) - gold layer
- Unit Tests - Idempotency (gold layer)
- Integration Testing - design detailed test approach and come up with specific test cases
- Smoke Tests / Observability - design detailed test approach and come up with specific test cases

## Test-Driven Development (TDD)

### General tasks

- General research about how to test Scraper e.g. handover to session headers for data URL, cloudflare challenge, etc.

### Pre-Commit Hook

#### Unit Tests
- entity_resolution - need to do that after notebook
- idempotency - can only do bronze layer now. need to come back to silver/gold later
- scd2 - could probably model dim injuries now given that we have raw injury data
- Unit Tests for scraper cookie handover thing?

#### Contract Tests
- Start with Bronze layer
- Maybe consider doing Silver layer after data modelling complete

### Pre-Push Hook

#### Integration Tests
- Need to design test approach for integration tests and come up with specific test cases

Smoke 


## Other Activities

- Entity Resolution Notebook
- Data Modelling - Silver layer