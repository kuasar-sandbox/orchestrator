#!/usr/bin/env bash
# Isolated real PostgreSQL, using only an image admitted by source preparation.
# The caller owns cleanup and supplies a private WORK directory.
start_test_postgres() {
    : "${ORCHESTRATOR_POSTGRES_IMAGE:?prepared PostgreSQL image is required}"
    : "${WORK:?private WORK directory is required}"
    docker image inspect "$ORCHESTRATOR_POSTGRES_IMAGE" >/dev/null 2>&1 || return 1
    local identity port password
    identity="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
    password="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
    TEST_POSTGRES_NETWORK="$(docker network create --driver bridge --label kuasar.test=cluster-postgres \
        "kuasar-cluster-pg-$identity" 2>"$WORK/postgres-network.private")" || return 1
    TEST_POSTGRES_CONTAINER="$(POSTGRES_PASSWORD="$password" docker create \
        --name "kuasar-cluster-pg-$identity" --network "$TEST_POSTGRES_NETWORK" --publish 127.0.0.1::5432 \
        --label kuasar.test=cluster-postgres --env POSTGRES_PASSWORD \
        --env POSTGRES_DB=cluster_test "$ORCHESTRATOR_POSTGRES_IMAGE" \
        2>"$WORK/postgres-container.private")" || return 1
    docker start "$TEST_POSTGRES_CONTAINER" >"$WORK/postgres-start.private" 2>&1 || return 1
    port="$(docker port "$TEST_POSTGRES_CONTAINER" 5432/tcp)" || return 1
    [[ "$port" =~ ^127\.0\.0\.1:([0-9]+)$ ]] || return 1
    port="${BASH_REMATCH[1]}"
    export STORAGE_TEST_POSTGRES_DSN="postgresql://postgres:$password@127.0.0.1:$port/cluster_test?sslmode=disable"
    local attempt
    for attempt in $(seq 1 120); do
        # Probe actual committed transactions, rather than accepting a listening
        # socket or pg_isready while initdb is still switching to the final DB.
        if timeout --kill-after=1s 3 docker exec --env "PGPASSWORD=$password" "$TEST_POSTGRES_CONTAINER" psql -X -h 127.0.0.1 -U postgres -d cluster_test \
                --set ON_ERROR_STOP=on --tuples-only --no-align \
                --command 'CREATE TABLE IF NOT EXISTS fixture_probe (id integer PRIMARY KEY); INSERT INTO fixture_probe VALUES (1) ON CONFLICT DO NOTHING;' \
                --command 'SELECT id FROM fixture_probe WHERE id=1;' \
                --command 'DROP TABLE fixture_probe;' >"$WORK/postgres-ready.private" 2>&1 \
                && grep -Fxq 1 "$WORK/postgres-ready.private"; then
            return 0
        fi
        sleep 0.25
    done
    return 1
}

stop_test_postgres() {
    local result=0
    if [ -n "${TEST_POSTGRES_CONTAINER:-}" ]; then
        docker rm -fv "$TEST_POSTGRES_CONTAINER" >/dev/null 2>&1 || result=1
    fi
    if [ -n "${TEST_POSTGRES_NETWORK:-}" ]; then
        docker network rm "$TEST_POSTGRES_NETWORK" >/dev/null 2>&1 || result=1
    fi
    TEST_POSTGRES_CONTAINER=""
    TEST_POSTGRES_NETWORK=""
    unset STORAGE_TEST_POSTGRES_DSN
    return "$result"
}
