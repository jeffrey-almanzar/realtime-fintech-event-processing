# The migrate tool with this repo's migrations baked in.
FROM migrate/migrate:v4.18.1
COPY migrations /migrations
