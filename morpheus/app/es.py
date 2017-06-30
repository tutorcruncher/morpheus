import asyncio
import json
import logging

from .settings import Settings
from .utils import ApiSession

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearch(ApiSession):
    def __init__(self, settings: Settings, loop=None):
        self.settings = settings
        super().__init__(settings.elastic_url, settings, loop)

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

        for index_name, mapping in MAPPINGS.items():
            r = await self.get(index_name, allowed_statuses=(200, 404))
            if r.status != 404:
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
                        'properties': mapping,
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


KEYWORD = {'type': 'keyword'}
DATE = {'type': 'date'}
TEXT = {'type': 'text'}
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
        'events': {
            'properties': {
                'ts': DATE,
                'status': KEYWORD,
                'extra': {
                    'type': 'object',
                    'dynamic': 'true',
                },
            }
        },
    },
}
