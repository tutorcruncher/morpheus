import asyncio
import json
import logging
from datetime import datetime
from time import time

from arq import create_pool_lenient

from .settings import Settings
from .utils import THIS_DIR, ApiError, ApiSession

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearch(ApiSession):  # pragma: no cover
    def __init__(self, settings: Settings, loop=None):
        self.settings = settings
        super().__init__(settings.elastic_url, settings, loop)

    async def set_license(self):
        license_file = (THIS_DIR.resolve() / '..' / 'es-license' / 'license.json').resolve()
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

    async def create_snapshot_repo(self, delete_existing=False):
        r = await self.get(f'/_snapshot/{self.settings.snapshot_repo_name}', allowed_statuses=(200, 404))
        if r.status == 200:
            if delete_existing:
                main_logger.warning('snapshot repo already exists, deleting it, response: %s', await r.text())
                await self.delete(f'/_snapshot/{self.settings.snapshot_repo_name}')
            else:
                data = await r.json()
                main_logger.info('snapshot repo already exists, not creating it, '
                                 'response: %s', json.dumps(data, indent=2))
                return data[self.settings.snapshot_repo_name]['type'], False

        if all((self.settings.s3_access_key, self.settings.s3_secret_key)):
            bucket = f'{self.settings.snapshot_repo_name}-snapshots'
            main_logger.info('s3 credentials set, creating s3 repo, bucket: %s', bucket)
            snapshot_type = 's3'
            settings = {
                'bucket': bucket,
                'access_key': self.settings.s3_access_key,
                'secret_key': self.settings.s3_secret_key,
                'endpoint': 's3-eu-west-1.amazonaws.com',
                'compress': True,
            }
        else:
            main_logger.info('s3 credentials not set, creating fs repo')
            snapshot_type = 'fs'
            settings = {
                'location': self.settings.snapshot_repo_name,
                'compress': True,
            }
        await self.put(f'/_snapshot/{self.settings.snapshot_repo_name}', type=snapshot_type, settings=settings)
        main_logger.info('snapshot %s created successfully using %s', self.settings.snapshot_repo_name, snapshot_type)
        return snapshot_type, True

    async def create_snapshot(self):
        main_logger.info('creating elastic search snapshot...')
        r = await self.put(
            f'/_snapshot/{self.settings.snapshot_repo_name}/'
            f'snapshot-{datetime.now():%Y-%m-%d_%H-%M-%S}?wait_for_completion=true',
            timeout_=1000,
        )
        main_logger.info('snapshot created: %s', json.dumps(await r.json(), indent=2))

    async def restore_list(self):
        r = await self.get(f'/_snapshot/{self.settings.snapshot_repo_name}/_all')
        main_logger.info(json.dumps(await r.json(), indent=2))

    async def restore_snapshot(self, snapshot_name):
        for index_name in MAPPINGS.keys():
            await self.post(f'{index_name}/_close')

        main_logger.info('indices closed. Restoring backup %s, this may take some time...', snapshot_name)
        start = time()
        r = await self.post(
            f'/_snapshot/{self.settings.snapshot_repo_name}/{snapshot_name}/_restore?wait_for_completion=true',
            timeout_=None,
        )
        main_logger.info(json.dumps(await r.json(), indent=2))

        main_logger.info('restore complete in %0.2fs, opening indices...', time() - start)
        for index_name in MAPPINGS.keys():
            await self.post(f'{index_name}/_open')

    async def _patch_update_mappings(self):
        for index_name, mapping_properties in MAPPINGS.items():

            r = await self.get(f'{index_name}/_mapping')
            all_mappings = await r.json()
            types = list(all_mappings[index_name]['mappings'].keys())

            await self.post(f'{index_name}/_close')
            for t in types:
                main_logger.info('updating mapping for "%s/%s"...', index_name, t)
                await self.put(f'{index_name}/_mapping/{t}', properties=mapping_properties)

            main_logger.info('%d types updated for %s, re-opening index', len(types), index_name)
            await self.post(f'{index_name}/_open')

    async def _patch_copy_events(self):
        r = await self.get(f'messages/_mapping')
        all_mappings = await r.json()
        redis_pool = await create_pool_lenient(self.settings.redis_settings, loop=None)
        async with redis_pool.get() as redis:
            for t in all_mappings['messages']['mappings']:
                r = await self.get(f'/messages/{t}/_search?scroll=2m', sort=['_doc'], query={'match_all': {}},
                                   size=1000)
                data = await r.json()
                scroll_id = data['_scroll_id']
                main_logger.info(f'messages/{t} {data["hits"]["total"]} messages to move events for')
                added, skipped = 0, 0
                set_key = f'event-set-{t}'
                events = set(await redis.smembers(set_key, encoding='utf8'))
                for i in range(int(1e9)):
                    rows = set()
                    for hit in data['hits']['hits']:
                        msg_id = hit['_id']
                        for event in hit['_source'].get('events', []):
                            row = json.dumps({'message': msg_id, **event}, sort_keys=True)
                            if row in events:
                                skipped += 1
                            else:
                                added += 1
                                rows.add(row)
                    r = await self.get('_search/scroll', scroll='2m', scroll_id=scroll_id, timeout_=10)
                    data = await r.json()
                    main_logger.info('  %d: %d events added, %d skipped, adding %d rows', i, added, skipped, len(rows))
                    if rows:
                        await asyncio.gather(
                            redis.sadd(set_key, *rows),
                            redis.expire(set_key, 86400),
                        )
                    if not data['hits']['hits']:
                        break
                main_logger.info(f'messages/{t} {added} events added, {skipped} skipped')

        redis_pool.close()
        await redis_pool.wait_closed()
        await redis_pool.clear()

    async def _patch_create_events(self):
        r = await self.get(f'messages/_mapping')
        all_mappings = await r.json()
        redis_pool = await create_pool_lenient(self.settings.redis_settings, loop=None)
        async with redis_pool.get() as redis:
            async def bulk_insert(rows):
                post_data = '\n'.join(rows)
                start = time()
                async with self.session.post(self.root + '_bulk', data=post_data, timeout=3600,
                                             headers={'Content-Type': 'application/x-ndjson'}) as r:
                    if r.status != 200:
                        raise ApiError('post', '_bulk', rows, r, await r.text())
                return time() - start

            for t in all_mappings['messages']['mappings']:
                set_key = f'event-set-{t}'
                for i in range(int(1e9)):
                    insert_rows = []
                    rows = await redis.srandmember(set_key, count=200, encoding='utf8')
                    if not rows:
                        break
                    for r in rows:
                        insert_rows.append(json.dumps({'index': {'_index': 'events', '_type': t}}))
                        insert_rows.append(r)
                    dur = await bulk_insert(insert_rows)
                    await redis.srem(set_key, *rows)
                    if i % 50 == 0:
                        remaining = await redis.scard(set_key)
                        main_logger.info('events/%s inserted %d rows in %0.2fs, remaining %d',
                                         t, len(insert_rows), dur, remaining)
        redis_pool.close()
        await redis_pool.wait_closed()
        await redis_pool.clear()


KEYWORD = {'type': 'keyword'}
DATE = {'type': 'date'}
TEXT = {'type': 'text'}
DYNAMIC = {'type': 'object', 'dynamic': 'true'}
MAPPINGS = {
    'messages': {
        'group_id': KEYWORD,
        'company': KEYWORD,
        'method': KEYWORD,
        'send_ts': DATE,
        'update_ts': DATE,
        'status': KEYWORD,
        'to_first_name': KEYWORD,
        'to_last_name': KEYWORD,
        'to_user_link': KEYWORD,
        'to_address': KEYWORD,
        'from_email': KEYWORD,
        'from_name': KEYWORD,
        'tags': KEYWORD,
        'subject': TEXT,
        'body': TEXT,
        'attachments': KEYWORD,
        'cost': {
          'type': 'scaled_float',
          'scaling_factor': 1000,
        },
        'extra': DYNAMIC,
        'events': {
            'properties': {
                'ts': DATE,
                'status': KEYWORD,
                'extra': DYNAMIC,
            }
        },
    },
    'links': {
        'token': KEYWORD,
        'url': KEYWORD,
        'company': KEYWORD,
        'send_method': KEYWORD,
        'send_message_id': KEYWORD,
        'expires_ts': DATE,
    },
    'events': {
        'message': KEYWORD,
        'ts': DATE,
        'status': KEYWORD,
        'extra': DYNAMIC,
    }
}
