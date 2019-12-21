# Event Scheduler

## Specifications
- Service to allow scheduling events with information fetched using callbacks.
- Allow polling for information update into the database and reflect the changes to scheduled *Event*.
- Service will run completely independent of main application.
    - Allow fetching for newly added schedule information from database. 