psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" buoy_retriever <<EOF

-- create the app schema
CREATE SCHEMA buoy_retriever AUTHORIZATION buoy_retriever;

-- Explicitly set the search path to prefer buoy_retriever schema
ALTER ROLE buoy_retriever SET search_path TO "buoy_retriever";

-- Revoke the default create grant for of any objects in public
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
EOF