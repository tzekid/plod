# A database of the run's own, and the example's own receiver as its webhook.
rm -f "$out/notes.db" "$out/notes.db-wal" "$out/notes.db-shm"
export NOTES_DB="$out/notes.db"
export NOTES_WEBHOOK="http://127.0.0.1:$port/hooks/receive"
