from buildpg import V, Values
from buildpg.asyncpg import BuildPgConnection
from buildpg.clauses import Where


async def get_or_create_company(conn: BuildPgConnection, company_code):
    company_id = await conn.fetchval_b('select id from companies where code = :code', code=company_code)
    if not company_id:
        company_id = await conn.fetchval_b(
            'insert into companies (code) values :values returning id', values=Values(code=company_code)
        )
    return company_id


async def get_sms_spend(conn: BuildPgConnection, *, company_id, method, start, end):
    # noinspection PyChainedComparisons
    where = Where(
        (V('method') == method) & (V('company_id') == company_id) & (start <= V('send_ts')) & (V('send_ts') < end)
    )
    return await conn.fetchval_b('select sum(cost) from messages :where', where=where)
