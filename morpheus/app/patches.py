from time import time

from atoolbox import patch
from atoolbox.db.helpers import run_sql_section


@patch
async def run_logic_sql(conn, settings, **kwargs):
    """
    run the "logic" section of models.sql
    """
    await run_sql_section('logic', settings.sql_path.read_text(), conn)


async def print_run_sql(conn, sql):
    print(f'running {sql}...')
    start = time()
    await conn.execute(sql)
    print(f'completed in {time() - start:0.1f}s')


@patch
async def performance_step1(conn, settings, **kwargs):
    """
    First step to changing schema to improve performance. THIS WILL BE VERY SLOW, but can be run in the background.
    """
    await print_run_sql(conn, "SET lock_timeout TO '2s'")
    await print_run_sql(conn, 'create extension if not exists btree_gin;')
    await print_run_sql(
        conn,
        """
        CREATE TABLE companies (
          id SERIAL PRIMARY KEY,
          code VARCHAR(63) NOT NULL UNIQUE
        );
        """,
    )
    await print_run_sql(
        conn,
        """
        INSERT INTO companies (code)
        SELECT DISTINCT company
        FROM message_groups;
        """,
    )

    await print_run_sql(conn, 'ALTER TABLE message_groups ADD company_id INT REFERENCES companies ON DELETE RESTRICT')
    await print_run_sql(conn, 'UPDATE message_groups g SET company_id=c.id FROM companies c WHERE g.company=c.code')

    await print_run_sql(
        conn,
        """
        ALTER TABLE messages ADD COLUMN company_id INT REFERENCES companies ON DELETE RESTRICT;
        ALTER TABLE messages ADD COLUMN method SEND_METHODS;
        """,
    )
    await print_run_sql(
        conn,
        """
        UPDATE messages m
        SET company_id=g.company_id, method=g.method
        FROM message_groups g
        WHERE m.group_id=g.id;
        """,
    )


@patch(direct=True)
async def performance_step2(conn, settings, **kwargs):
    """
    Second step to changing schema to improve performance. THIS WILL BE VERY SLOW, but can be run in the background.
    """
    await print_run_sql(
        conn,
        """
        DROP INDEX CONCURRENTLY IF EXISTS message_status;
        DROP INDEX CONCURRENTLY IF EXISTS message_group_id;
        DROP INDEX CONCURRENTLY IF EXISTS event_ts;
        DROP INDEX CONCURRENTLY IF EXISTS link_message_id;
        """,
    )
    await print_run_sql(
        conn, 'CREATE INDEX CONCURRENTLY message_group_company_id ON message_groups USING btree (company_id)'
    )
    await print_run_sql(
        conn,
        """
        DROP INDEX CONCURRENTLY IF EXISTS message_update_ts;
        CREATE INDEX CONCURRENTLY message_update_ts ON messages USING btree (update_ts desc);
        """,
    )
    await print_run_sql(
        conn,
        """
        DROP INDEX CONCURRENTLY IF EXISTS message_tags;
        CREATE INDEX CONCURRENTLY message_tags ON messages USING gin (tags, method, company_id);
        """,
    )
    await print_run_sql(
        conn,
        """
        DROP INDEX CONCURRENTLY IF EXISTS message_vector;
        CREATE INDEX CONCURRENTLY message_vector ON messages USING gin (vector, method, company_id);
        """,
    )
    await print_run_sql(
        conn,
        """
        DROP INDEX CONCURRENTLY IF EXISTS message_company_method;
        CREATE INDEX CONCURRENTLY message_company_method ON messages USING btree (method, company_id, id);
        """,
    )


@patch
async def performance_step3(conn, settings, **kwargs):
    """
    Third step to changing schema to improve performance. This should not be too slow, but will LOCK ENTIRE TABLES.
    """
    print('create the table companies...')
    await print_run_sql(conn, "SET lock_timeout TO '2s'")
    await print_run_sql(conn, 'LOCK TABLE companies IN SHARE MODE;')

    await print_run_sql(
        conn,
        """
        INSERT INTO companies (code)
        SELECT DISTINCT company FROM message_groups
        ON CONFLICT (code) DO NOTHING;
        """,
    )

    await print_run_sql(conn, 'LOCK TABLE message_groups IN SHARE MODE;')
    await print_run_sql(
        conn,
        """
        UPDATE message_groups g SET company_id=c.id
        FROM companies c WHERE g.company=c.code AND g.company_id IS NULL
        """,
    )
    await print_run_sql(conn, 'ALTER TABLE message_groups ALTER company_id SET NOT NULL')
    await print_run_sql(conn, 'ALTER TABLE message_groups DROP company')
    await print_run_sql(conn, 'ALTER TABLE message_groups RENAME method TO message_method')

    await print_run_sql(conn, 'LOCK TABLE messages IN SHARE MODE;')
    await print_run_sql(
        conn,
        """
        UPDATE messages m
        SET company_id=g.company_id, method=g.message_method
        FROM message_groups g
        WHERE m.group_id=g.id AND m.company_id IS NULL
        """,
    )
    await print_run_sql(conn, 'ALTER TABLE messages ALTER COLUMN company_id SET NOT NULL')
    await print_run_sql(conn, 'ALTER TABLE messages ALTER COLUMN method SET NOT NULL')
