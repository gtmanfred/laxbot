'''
Slack Bot that prints on the console.
'''
import aiohttp
import aioredis
import asyncio
import json
import logging
import random

import config

logging.basicConfig(level=getattr(logging, getattr(config, 'LOGLEVEL', 'warning').upper(), logging.WARNING))
log = logging.getLogger(__name__)


class RateLimit(object):

    _pool = None
    bad_messages = (
        'bot_message',
        'message_deleted',
    )

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(log.getEffectiveLevel() <= logging.DEBUG)

    async def start(self):
        while True:
            channel = asyncio.Queue()
            done, pending = await asyncio.wait(
                (self.bot(channel.put), self.consumer(channel.get), self.ping()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if hasattr(self, 'ws'):
                del self.ws
            await asyncio.sleep(5)

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

    async def warn_user(self, message):
        channel = await self.api_call('conversations.info', {'channel': message['channel']})
        imid = await self.api_call('conversations.open', {'users': [message['user']]})
        await self.api_call(
            'chat.postMessage',
            {
                'channel': imid['channel']['id'],
                'text': f'Please slow down. Message removed #{channel["channel"]["name"]}: {message["text"]}',
            },
        )

    async def consumer(self, get):
        '''Display the message.'''
        while True:
            message = await get()
            if message.get('type') == 'message' and message.get('subtype') not in self.bad_messages:
                log.info('Message: %s', message)
                pool = await self.pool
                nummsgs = await pool.execute('incr', message['user'])
                if nummsgs == 1:
                    await pool.execute('expire', message['user'], config.MAXTIMEOUT)
                if nummsgs > config.MAXMSGS:
                    self.loop.create_task(
                        self.api_call('chat.delete', {'channel': message['channel'], 'ts': message['ts']})
                    )
                    self.loop.create_task(self.warn_user(message))
            elif message.get('type') == 'pong':
                self.msgid = message['reply_to']
            else:
                log.debug('Debug Message: %s', message)

    async def ping(self):
        '''ping websocket'''
        while not hasattr(self, 'ws'):
            await asyncio.sleep(1)
        while True:
            msgid = random.randrange(10000)
            try:
                await self.ws.send_str(json.dumps({'type': 'ping', 'id': msgid}))
            except RuntimeError:
                break
            await asyncio.sleep(20)
            if msgid != self.msgid:
                break

    async def bot(self, put):
        '''Create a bot that joins Slack.'''
        rtm = await self.api_call('rtm.connect')
        if not rtm['ok']:
            raise Exception('Error connecting to RTM.')

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(rtm['url']) as self.ws:
                async for msg in self.ws:
                    await put(json.loads(msg.data))


if __name__ == '__main__':
    bot = RateLimit()
    bot.loop.run_until_complete(bot.start())
