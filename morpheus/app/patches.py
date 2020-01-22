from atoolbox import patch
from atoolbox.db.helpers import run_sql_section


@patch
async def run_logic_sql(conn, settings, **kwargs):
    """
    run the "logic" section of models.sql
    """
    await run_sql_section('logic', settings.sql_path.read_text(), conn)


@patch
async def improve_indexes(conn, settings, **kwargs):
    """
    Improve indexes, dropping unused ones and adding more
    """
    await conn.execute('DROP INDEX IF EXISTS message_status')
    await conn.execute('DROP INDEX IF EXISTS event_ts')
    await conn.execute('DROP INDEX IF EXISTS link_message_id')
    await conn.execute('DROP INDEX IF EXISTS message_group_id')
    await conn.execute('DROP INDEX IF EXISTS message_send_ts')


@patch
async def add_companies(conn, settings, **kwargs):
    """
    add companies table, and modify the database to use it companies
    """
    print('create the table companies...')
    await conn.execute("SET lock_timeout TO '2s'")
    await run_sql_section('companies', settings.sql_path.read_text(), conn)
    print('populate the table companies...')
    await conn.execute(
        """
        INSERT INTO companies (name)
        SELECT DISTINCT company
        FROM message_groups;
        """
    )
    print('add column message_groups.company_id...')
    await conn.execute(
        """
        ALTER TABLE message_groups ADD company_id INT REFERENCES companies ON DELETE RESTRICT;
        """
    )
    print('populate column message_groups.company_id...')
    await conn.execute(
        """
        UPDATE message_groups g SET company_id=c.id FROM companies c WHERE g.company=c.name;
        """
    )
    print('modify column message_groups.company_id to not be nullable...')
    await conn.execute(
        """
        ALTER TABLE message_groups ALTER company_id SET NOT NULL;
        """
    )
    print('drop the now redundant column message_groups.company...')
    await conn.execute(
        """
        ALTER TABLE message_groups DROP company;
        """
    )
    print('rename teh column message_groups.method to message_groups.message_method...')
    await conn.execute(
        """
        ALTER TABLE message_groups RENAME method TO message_method;
        """
    )
    print('add columns messages.companies and messages.method...')
    await conn.execute(
        """
        ALTER TABLE messages ADD COLUMN company_id INT REFERENCES companies ON DELETE RESTRICT;
        ALTER TABLE messages ADD COLUMN method SEND_METHODS;
        """
    )
    print('populate columns messages.company_id and messages.method...')
    await conn.execute(
        """
        UPDATE messages m
        SET company_id=g.company_id, method=g.message_method
        FROM message_groups g
        WHERE m.group_id=g.id;
        """
    )
    print('modify columns messages.company_id and messages.method to not be nullable...')
    await conn.execute(
        """
        ALTER TABLE messages ALTER COLUMN company_id SET NOT NULL;
        ALTER TABLE messages ALTER COLUMN method SET NOT NULL;
        """
    )
