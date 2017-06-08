import asyncio
import logging

from .settings import Settings
from .utils import ApiSession

main_logger = logging.getLogger('morpheus.elastic')


class ElasticSearch(ApiSession):
    def __init__(self, settings: Settings, loop=None):
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
                    main_logger.warning('deleting index %s', index_name)
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
        'to_email': KEYWORD,
        'from_email': KEYWORD,
        'from_name': KEYWORD,
        'tags': KEYWORD,
        'subject': TEXT,
        'body': TEXT,
        'attachments': KEYWORD,
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
