from atoolbox import patch


@patch
async def run_logic_sql(conn, settings, **kwargs):
    """
    run logic.sql code.
    """
    await conn.execute(settings.logic_sql_path.read_text())
