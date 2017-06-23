from morpheus.app.worker import AuxActor


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
