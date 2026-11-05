#!/bin/bash
# The MySQL entrypoint chowns secure_file_priv before executing init scripts.
# Keep the host dataset read-only and copy it into the server's import directory.
cp /datasets/athlete_event_results.csv /var/lib/mysql-files/athlete_event_results.csv
