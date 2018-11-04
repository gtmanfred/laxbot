'''
Slack Bot that prints on the console.
'''
import aiohttp
import aioredis
import asyncio
import json
import logging
import sys
import traceback

import config

logging.basicConfig(level=logging.DEBUG)


class RateLimit(object):

    _pool = None

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(False)

    def start(self):
        channel = asyncio.Queue()
        self.loop.create_task(asyncio.wait((self.bot(channel.put), self.consumer(channel.get))))
        self.loop.run_forever()

    @property
    async def pool(self):
        if self._pool is None:
            self._pool = await aioredis.create_redis('redis://localhost', loop=self.loop)
        return self._pool

    async def api_call(self, method, data=None, token=config.TOKEN):
        '''Slack API call.'''
        async with aiohttp.ClientSession(loop=self.loop) as session:
            if data is None:
                data = {}
            data['username'] = 'ratelimiter'
            data['icon_url'] = 'https://sanantoniodevs.com/images/sadev500w.png'
            form = aiohttp.FormData(data)
            form.add_field('token', token)
            async with session.post('https://slack.com/api/{0}'.format(method), data=form) as response:
                if response.status == 429:
                    await asyncio.sleep(
                        int(response.headers['Retry-After'])
                    )
                    return await self.api_call(method, data)
                if response.status != 200:
                    raise Exception(
                        '{0} with {1} failed.'.format(method, data)
                    )
                return await response.json()

    async def consumer(self, get):
        '''Display the message.'''
        message = None
        try:
            while True:
                message = await get()
                if message.get('type') == 'message':
                    print(message)
                    pool = await self.pool
                    nummsgs = await pool.execute('incr', message['user'])
                    if nummsgs == 1:
                        await pool.execute('expire', message['user'], config.MAXTIMEOUT)
                    if nummsgs > config.MAXMSGS:
                        self.loop.create_task(self.api_call('chat.delete', {'channel': message['channel'], 'ts': message['ts']}))
                        self.loop.create_task(self.api_call('chat.postMessage', {'channel': message['channel'], 'text': 'test text'}))
                else:
                    print(message, file=sys.stderr)
        except Exception:
            traceback.print_exc()

    async def bot(self, put):
        '''Create a bot that joins Slack.'''
        rtm = await self.api_call('rtm.start')
        if not rtm['ok']:
            raise Exception('Error connecting to RTM.')

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(rtm['url']) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.text:
                        message = json.loads(msg.data)
                        await put(message)
                    else:
                        print('ERROR')
                        break


if __name__ == '__main__':
    bot = RateLimit()
    bot.start()
