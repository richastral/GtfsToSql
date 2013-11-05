GtfsToSql
=========
Parses a GTFS feed into an SQL database

Installation
------------
If you're using Eclipse, you need to:

1. File, Import
2. Select **Existing projects into workspace**
3. **Select root directory**

Usage
-----
`java -jar GtfsToSql.jar -s /path/to/database/sqlite -g /path/to/extracted/gtfs/`

Notes
-----
* Only supports Sqlite currently
* Sqlite file must not already exist
* GTFS file must be extracted already
* All columns are mapped saved as 'text' (that is, not parsed, modified or typecast)

Table names are the same as filename in the GTFS file (without the `.txt`), and an additional table called `_gtfs_issues` is also created that records issues encountered while reading in the files.
