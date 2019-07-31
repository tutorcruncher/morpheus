from atoolbox import patch
from atoolbox.db.helpers import run_sql_section


@patch
async def run_logic_sql(conn, settings, **kwargs):
    """
    run the "logic" section of models.sql
    """
    await run_sql_section('logic', settings.sql_path.read_text(), conn)
