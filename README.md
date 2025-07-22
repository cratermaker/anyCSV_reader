# anyCSV_reader
A robust command-line tool that imports CSV files into a local SQLite database and allows you to efficiently search for specific data across all imported datasets. 

Automatic Import: Scans and imports all .csv files in a selected folder.

Smart Hashing: Avoids duplicate imports using file content and row-level hashes.

Flexible Parsing: Handles files with or without headers and recovers partial data from malformed or incomplete files.

Search Engine: Supports multi-keyword full-table search across all columns (excluding internal hashes).

Automatic Table Naming: Each CSV file becomes a separate SQLite table, automatically named and indexed.

Optimized Storage: Only unique rows are stored based on content hash.
