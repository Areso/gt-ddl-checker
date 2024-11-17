# gt-ddl-checker

curl -X POST -d '{"migration":"ALTER TABLE titles ADD INDEX idx_to_date(to_date)", "cluster":"employees"}' localhost:5555/check_migration