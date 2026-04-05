-- Grant privileges on a specific table
GRANT USAGE ON SCHEMA {{SCHEMA}} TO {{USERNAME}};
GRANT {{PRIVILEGES}} ON {{SCHEMA}}.{{TABLE}} TO {{USERNAME}}{{GRANT_OPTION}};
