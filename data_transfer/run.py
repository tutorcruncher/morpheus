#!/usr/bin/env python3.6
import asyncio
import json
import logging
import os
import sys
from datetime import timezone
from pathlib import Path
from time import time

import uvloop
from arq.utils import from_unix_ms
from buildpg import asyncpg, Values, MultipleValues
from devtools import debug
from tqdm import tqdm

from morpheus.app.db import prepare_database
from morpheus.app.logs import setup_logging
from morpheus.app.models import SendMethod
from morpheus.app.settings import Settings
from morpheus.app.utils import ApiSession

main_logger = logging.getLogger('morpheus.elastic')
elastic_url = 'http://localhost:9200'
s3_access_key: str = os.getenv('s3_access_key', None)
s3_secret_key: str = os.getenv('s3_secret_key', None)
snapshot_repo_name = 'morpheus'

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

    async def create_snapshot_repo(self):
        r = await self.get(f'/_snapshot/{snapshot_repo_name}', allowed_statuses=(200, 404))
        if r.status == 200:
            data = await r.json()
            main_logger.info('snapshot repo already exists, not creating it, '
                             'response: %s', json.dumps(data, indent=2))
            return data[snapshot_repo_name]['type'], False

        bucket = f'{snapshot_repo_name}-snapshots'
        main_logger.info('s3 credentials set, creating s3 repo, bucket: %s', bucket)
        snapshot_type = 's3'
        settings = {
            'bucket': bucket,
            'access_key': s3_access_key,
            'secret_key': s3_secret_key,
            'endpoint': 's3-eu-west-1.amazonaws.com',
            'compress': True,
            'readonly': True,
        }
        await self.put(f'/_snapshot/{snapshot_repo_name}', type=snapshot_type, settings=settings)
        main_logger.info('snapshot %s created successfully using %s', snapshot_repo_name, snapshot_type)
        return snapshot_type, True

    async def restore_list(self):
        r = await self.get(f'/_snapshot/{snapshot_repo_name}/_all')
        d = await r.json()
        snapshots = d['snapshots']
        debug(snapshots_count=len(snapshots), last_5=snapshots[-5:])

    async def restore_snapshot(self, snapshot_name):
        for index_name in MAPPINGS.keys():
            await self.post(f'{index_name}/_close')

        main_logger.info('indices closed. Restoring backup %s, this may take some time...', snapshot_name)
        start = time()
        r = await self.post(
            f'/_snapshot/{snapshot_repo_name}/{snapshot_name}/_restore?wait_for_completion=true',
            timeout_=None,
        )
        main_logger.info(json.dumps(await r.json(), indent=2))

        main_logger.info('restore complete in %0.2fs, opening indices...', time() - start)
        for index_name in MAPPINGS.keys():
            await self.post(f'{index_name}/_open')


async def create_message_group(group_uuid, message, pg):
    source = message['_source']
    group_values = Values(
        uuid=group_uuid,
        company=source['company'],
        method=message['_type'],
        created_ts=from_unix_ms(source['send_ts']).replace(tzinfo=timezone.utc),
        from_email=source.get('from_email'),
        from_name=source.get('from_name'),
    )
    async with pg.acquire() as conn:
        group_id = await conn.fetchval_b(
            """
            insert into message_groups (:values__names) values :values
            on conflict (uuid) do nothing
            returning id
            """,
            values=group_values,
        )
        if not group_id:
            group_id = await conn.fetchval('select id from message_groups where uuid=$1', group_uuid)
        return group_uuid, group_id


async def process_messages(messages, pg):
    if not messages:
        return
    coros = []
    group_ids = set()
    for message in messages:
        group_id = message['_source']['group_id']
        if group_id in group_ids:
            continue
        coros.append(create_message_group(group_id, message, pg))

    del group_ids
    group_ids_lookup = dict(await asyncio.gather(*coros))
    values = MultipleValues(*[
        Values(
            external_id=msg['_id'],
            group_id=group_ids_lookup[msg['_source']['group_id']],
            send_ts=from_unix_ms(msg['_source']['send_ts']).replace(tzinfo=timezone.utc),
            update_ts=from_unix_ms(msg['_source']['update_ts']).replace(tzinfo=timezone.utc),
            status=msg['_source']['status'],
            to_first_name=msg['_source'].get('to_first_name'),
            to_last_name=msg['_source'].get('to_last_name'),
            to_user_link=msg['_source'].get('to_user_link'),
            to_address=msg['_source'].get('to_address'),
            tags=msg['_source'].get('tags'),
            subject=msg['_source'].get('subject'),
            body=msg['_source'].get('body'),
            attachments=msg['_source'].get('attachments'),
            cost=msg['_source'].get('cost'),
            extra=json.dumps(msg['_source'].get('extra')) if msg['_source'].get('extra') else None,
        ) for msg in messages
    ])
    async with pg.acquire() as conn:
        await conn.execute_b('insert into messages (:values__names) values :values', values=values)


async def transfer_messages(es, pg, loop):
    # clear the db before we begin
    print('deleting everything the the db...')
    await pg.execute('delete from message_groups')
    print('done')

    for m in reversed(SendMethod):
        r = await es.get(f'messages/{m.value}/_search?scroll=10m', size=1000, sort=[{'send_ts': 'desc'}])
        data = await r.json()
        total = data['hits']['total']
        scroll_id = data['_scroll_id']
        hits = data['hits']['hits']
        tasks = [loop.create_task(process_messages(hits, pg))]
        hit_count = len(hits)
        print('method: {}, messages: {}'.format(m.value, total))
        for i in tqdm(range(total // 1000 + 1), smoothing=0.1):
            es_query = {
                'scroll': '10m',
                'scroll_id': scroll_id,
            }
            r = await es.get(f'/_search/scroll', **es_query)
            data = await r.json()
            hits = data['hits']['hits']
            hit_count += len(hits)
            if hits:
                tasks.append(loop.create_task(process_messages(hits, pg)))
            else:
                break

            if i % 100 == 0:
                await asyncio.gather(*tasks)
                tasks = []

        print('method: {}, total hits: {}, {} tasks to complete'.format(m.value, hit_count, len(tasks)))
        await asyncio.gather(*tasks)


message_id_lookup = {}


async def process_events(events, pg):
    new_message_ids = set()
    for event in events:
        message_id = event['_source']['message_id']
        if message_id not in message_id_lookup:
            new_message_ids.add(new_message_ids)

    if new_message_ids:
        async with pg.acquire() as conn:
            v = await conn.fetch('select external_id, id from messages where external_id=any($1)', new_message_ids)
            message_id_lookup.update(dict(v))

    values = MultipleValues(*[
        Values(
            message_id=message_id_lookup[event['_source']['message_id']],
            status=event['_source']['status'],
            ts=from_unix_ms(event['_source']['ts']).replace(tzinfo=timezone.utc),
            extra=json.dumps(event['_source'].get('extra')) if event['_source'].get('extra') else None,
        ) for event in events
    ])
    async with pg.acquire() as conn:
        await conn.execute_b('insert into events (:values__names) values :values', values=values)


async def transfer_events(es, pg, loop):
    await pg.execute('DROP TRIGGER IF EXISTS update_message ON events;')

    try:
        for m in reversed(SendMethod):
            r = await es.get(f'events/{m.value}/_search?scroll=10m', size=1000, sort=[{'ts': 'desc'}])
            data = await r.json()
            total = data['hits']['total']
            scroll_id = data['_scroll_id']
            hits = data['hits']['hits']
            tasks = [loop.create_task(process_events(hits, pg))]
            hit_count = len(hits)
            print('method: {}, events: {}'.format(m.value, total))
            for i in tqdm(range(total // 1000 + 1), smoothing=0.1):
                es_query = {
                    'scroll': '10m',
                    'scroll_id': scroll_id,
                }
                r = await es.get(f'/_search/scroll', **es_query)
                data = await r.json()
                hits = data['hits']['hits']
                hit_count += len(hits)
                if hits:
                    tasks.append(loop.create_task(process_events(hits, pg)))
                else:
                    break

                if i % 100 == 0:
                    await asyncio.gather(*tasks)
                    tasks = []

            print('method: {}, total hits: {}, {} tasks to complete'.format(m.value, hit_count, len(tasks)))
            await asyncio.gather(*tasks)
    finally:
        await pg.execute('CREATE TRIGGER update_message AFTER INSERT ON events '
                         'FOR EACH ROW EXECUTE PROCEDURE update_message();')


async def main(loop):
    start = time()
    settings = Settings()
    setup_logging(settings)

    es = ElasticSearch(settings=settings, loop=loop)

    await prepare_database(settings, False)
    pg = await asyncpg.create_pool_b(dsn=settings.pg_dsn, min_size=100, max_size=400)
    try:
        if len(sys.argv) == 1:
            action = 'restore_list'
        else:
            action = sys.argv[1]
        if action == 'restore_list':
            await es.create_indices()
            await es.create_snapshot_repo()
            print('listing restores...')
            await es.restore_list()
        elif action == 'restore':
            snapshot = sys.argv[2]
            await es.restore_snapshot(snapshot)
        elif action == 'transfer_messages':
            await transfer_messages(es, pg, loop)
        elif action == 'transfer_events':
            await transfer_events(es, pg, loop)
        else:
            raise RuntimeError(f'unknown action: "{action}"')
    finally:
        await pg.close()
        await es.close()
        main_logger.info('time taken: %0.2fs', time() - start)


if __name__ == '__main__':
    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop_ = asyncio.get_event_loop()
    loop_.run_until_complete(main(loop_))
