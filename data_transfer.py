#!/usr/bin/env python3.6
import asyncio
import json
import logging
import sys
from pathlib import Path
from time import time

from buildpg import asyncpg
from devtools import debug

from morpheus.app.db import prepare_database
from morpheus.app.logs import setup_logging
from morpheus.app.settings import Settings
from morpheus.app.utils import ApiSession

main_logger = logging.getLogger('morpheus.elastic')
elastic_url = 'http://localhost:9200'
s3_access_key: str = None
s3_secret_key: str = None
snapshot_repo_name = 'morpheus'


class ElasticSearch(ApiSession):  # pragma: no cover
    def __init__(self, settings: Settings, loop=None):
        self.settings = settings
        super().__init__(elastic_url, settings, loop)

    async def set_license(self):
        license_file = Path('license.json').resolve()
        if not license_file.exists():
            main_logger.info('X license file "%s" does not exist, not setting license', license_file)
            return

        with license_file.open() as f:
            data = json.load(f)
        main_logger.info('settings elasticsearch license...')
        r = await self.put('_xpack/license?acknowledge=true', **data)
        main_logger.info('license set, response: %s', json.dumps(await r.json(), indent=2))

    async def create_indices(self, delete_existing=False):
        """
        Create mappings for indices, this method is "lenient",
        eg. it retries for 5 seconds if es appears to not be up yet
        """
        for i in range(50):
            r = await self.get('', allowed_statuses='*')
            if r.status == 200:
                break
            await asyncio.sleep(0.1, loop=self.loop)

        for index_name, mapping_properties in MAPPINGS.items():
            r = await self.get(index_name, allowed_statuses=(200, 404))
            if r.status == 200:
                if delete_existing:
                    main_logger.warning('deleting index "%s"', index_name)
                    await self.delete(index_name)
                else:
                    main_logger.info('elasticsearch index %s already exists, not creating', index_name)
                    continue
            main_logger.info('creating index %s...', index_name)
            await self.put(index_name, mappings={
                '_default_': {
                        'dynamic': 'strict',
                        'properties': mapping_properties,
                    }
                }
            )

    async def restore_list(self):
        r = await self.get(f'/_snapshot/{snapshot_repo_name}/_all')
        debug(snapshots=await r.json())

    async def restore_snapshot(self, snapshot_name):
        indexes = 'messages', 'links', 'events'
        for index_name in indexes:
            await self.post(f'{index_name}/_close')

        main_logger.info('indices closed. Restoring backup %s, this may take some time...', snapshot_name)
        start = time()
        r = await self.post(
            f'/_snapshot/{snapshot_repo_name}/{snapshot_name}/_restore?wait_for_completion=true',
            timeout_=None,
        )
        main_logger.info(json.dumps(await r.json(), indent=2))

        main_logger.info('restore complete in %0.2fs, opening indices...', time() - start)
        for index_name in indexes:
            await self.post(f'{index_name}/_open')


async def main(loop):
    settings = Settings()
    setup_logging(settings)

    es = ElasticSearch(settings=settings, loop=loop)

    await prepare_database(settings, False)
    pg = await asyncpg.create_pool_b(dsn=settings.pg_dsn, min_size=10)
    try:

        if len(sys.argv) == 1:
            action = 'restore_list'
        else:
            action = sys.argv[1]
        assert action in {'restore_list', 'restore'}, f'unknown action: "{action}"'
        if action == 'restore_list':
            await es.restore_list()
    finally:
        await pg.close()
        await es.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
