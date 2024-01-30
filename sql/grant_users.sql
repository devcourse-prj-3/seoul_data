-- 본 sql 실행 전 권한 부여가 가능한 계정으로 접속해야 함.

GRANT USAGE ON SCHEMA {schema_name} TO {user_account};
GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO {user_account};