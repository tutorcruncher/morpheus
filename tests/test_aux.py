import os

import pytest

from morpheus.app.worker import AuxActor


async def test_index(cli):
    r = await cli.get('/')
    assert r.status == 200
    assert 'Morpheus - The Greek God' in await r.text()


async def test_index_head(cli):
    r = await cli.head('/')
    assert r.status == 200
    assert '' == await r.text()


async def test_robots(cli):
    r = await cli.get('/robots.txt')
    assert r.status == 200
    assert 'User-agent: *' in await r.text()


async def test_favicon(cli):
    r = await cli.get('/favicon.ico', allow_redirects=False)
    assert r.status == 200
    assert 'image' in r.headers['Content-Type']  # value can vary


async def test_create_repo(cli, settings):
    es = cli.server.app['es']
    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}', allowed_statuses=(200, 404))
    if r.status == 200:
        await es.delete(f'/_snapshot/{settings.snapshot_repo_name}')

    type, created = await es.create_snapshot_repo()
    assert type == 'fs'
    assert created is True

    type, created = await es.create_snapshot_repo()
    assert type == 'fs'
    assert created is False

    type, created = await es.create_snapshot_repo(delete_existing=True)
    assert type == 'fs'
    assert created is True


@pytest.mark.skipif(not os.getenv('TRAVIS'),  reason='only run on travis')
async def test_run_snapshot(cli, settings, loop):
    es = cli.server.app['es']
    await es.create_snapshot_repo()

    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}/_all?pretty=true')
    print(await r.text())
    data = await r.json()
    snapshots_before = len(data['snapshots'])

    aux = AuxActor(settings=settings, loop=loop)
    await aux.startup()
    await aux.snapshot_es.direct()
    await aux.close(shutdown=True)

    r = await es.get(f'/_snapshot/{settings.snapshot_repo_name}/_all?pretty=true')
    print(await r.text())
    data = await r.json()
    assert len(data['snapshots']) == snapshots_before + 1
