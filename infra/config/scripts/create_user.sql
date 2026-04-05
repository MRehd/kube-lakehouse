-- Create or update user
DO $body$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{{NAME}}') THEN
    ALTER USER {{NAME}} WITH {{OPTIONS}};
  ELSE
    CREATE USER {{NAME}} WITH {{OPTIONS}};
  END IF;
END
$body$;
