GtfsToSql
=========

Parses a GTFS feed into an SQL database

Usage:

java -jar GtfsToSql.jar -s /path/to/database/sqlite -g /path/to/extracted/gtfs/

Notes:

* Only supports Sqlite currently
* Sqlite file must not already exist
* GTFS file must be extracted already
* All columns are mapped saved as 'text' (that is, not parsed, modified or typecast)
