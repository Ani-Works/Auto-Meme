import os
import time
import html
import json
import yaml
import asyncpraw
import aiohttp
import aiocron
import random
import logging
import asyncio
import tempfile
import functools
import mimetypes
import traceback
from decimal import Decimal
from bs4 import BeautifulSoup
from itertools import zip_longest
from urllib.parse import urlparse, urlunparse, urljoin
from telethon import TelegramClient, events
from telethon.utils import chunks, is_list_like
from telethon.tl.types import DocumentAttributeVideo

logging.basicConfig(level=logging.INFO)

mimetypes.init(['mime.types'])
with open('config.yaml') as file:
    config_data = yaml.safe_load(file)

tg_api_id = config_data['telegram']['api_id']
tg_api_hash = config_data['telegram']['api_hash']
bot_token = config_data['telegram'].get('bot_token')

reddit_client_id = config_data['reddit']['client_id']
reddit_client_secret = config_data['reddit']['client_secret']

unique_id = config_data['config'].get('unique_id')
storage_chat = config_data['config'].get('storage_chat')
storage_msg_id = config_data['config'].get('storage_message_id')
_bkup_subreddits = config_data['config'].get('subreddits')
_send_to_chats = send_to_chats = config_data['config']['send_to_chats']
if isinstance(_send_to_chats, list):
    send_to_chats = dict()
    for i in _send_to_chats:
        j = None
        if isinstance(i, dict):
            j = tuple(i.values())[0]
            i = tuple(i.keys())[0]
        if isinstance(j, list) or not j:
            j = {'subreddits': j, 'cron_duration': config_data['config']['cron_duration'],
                 'allow_selfposts': True, 'allow_nsfw': True,
                 'allow_spoilers': True, 'show_nsfw_warning': True,
                 'show_spoilers_warning': True, 'timeout': None}
        send_to_chats[i] = j
bot_admins = config_data['config']['bot_admins']

logging.basicConfig(level=logging.INFO)
async def main():
    _added_chats = []
    client = await TelegramClient('redditbot', tg_api_id, tg_api_hash).start(bot_token=bot_token)
    client.parse_mode = 'html'
    session = aiohttp.ClientSession()
    reddit = asyncpraw.Reddit(client_id=reddit_client_id, client_secret=reddit_client_secret, user_agent='linux:redditbot:v1.0.0 (by /u/the_blank_x)')
    try:
        if storage_chat and storage_msg_id:
            await (await client.get_messages(storage_chat, ids=storage_msg_id)).download_media('redditbot.json')
        with open('redditbot.json') as file:
            seen_posts = json.load(file)
        if isinstance(seen_posts, list):
            seen_posts = {'version': 0, 'chats': {'global': seen_posts}}
    except BaseException:
        logging.exception('Loading JSON')
        seen_posts = {'version': 0, 'chats': {'global': []}}
        # chat dict: {chatid: [array of submission ids]}

    uploading_lock = asyncio.Lock()
    async def write_seen_posts():
        with open('redditbot.json', 'w') as file:
            json.dump(seen_posts, file)
        if storage_chat and storage_msg_id:
            async with uploading_lock:
                await client.edit_message(storage_chat, storage_msg_id, file='redditbot.json')

    async def add_chat(chat, chat_data):
        global_sp = chat_sp = seen_posts['chats']['global']
        subreddits = chat_data['subreddits']
        if subreddits:
            chat = await client.get_peer_id(chat)
            if str(chat) not in seen_posts['chats']:
                seen_posts['chats'][str(chat)] = []
            chat_sp = seen_posts['chats'][str(chat)]
        else:
            subreddits = _bkup_subreddits.copy()
        cron_duration = chat_data['cron_duration']
        allow_selfposts = chat_data['allow_selfposts']
        allow_nsfw = chat_data['allow_nsfw']
        allow_spoilers = chat_data['allow_spoilers']
        show_nsfw_warning = chat_data['show_nsfw_warning']
        show_spoilers_warning = chat_data['show_spoilers_warning']
        timeout = chat_data.get('timeout')

        give_ups = set()
        async def _get_submission(unique_id):
            while unique_id not in give_ups:
                subreddit = await reddit.subreddit(random.choice(subreddits))
                random_post = await subreddit.random()
                cpid = cpp = None
                if random_post is None:
                    async for submission in subreddit.hot():
                        if unique_id in give_ups:
                            return
                        cpid = getattr(submission, 'crosspost_parent', None)
                        if cpid and getattr(random_post, 'crosspost_parent_list', None):
                            cpid = cpid[3:]
                        if submission.id in chat_sp + global_sp or cpid in chat_sp + global_sp:
                            continue
                        if not (allow_selfposts and allow_nsfw and allow_spoilers):
                            is_self = submission.is_self
                            nsfw = submission.over_18
                            spoilers = submission.spoiler
                            if cpid:
                                cpp = await reddit.submission(cpid)
                                if not allow_selfposts:
                                    is_self = cpp.is_self
                                if not (nsfw and allow_nsfw):
                                    nsfw = cpp.over_18
                                if not (spoilers and allow_spoilers):
                                    nsfw = cpp.spoiler
                            if is_self and not allow_selfposts:
                                continue
                            if nsfw and not allow_nsfw:
                                continue
                            if spoilers and not allow_spoilers:
                                continue
                        random_post = submission
                        break
                cpid = getattr(random_post, 'crosspost_parent', None)
                if cpid and getattr(random_post, 'crosspost_parent_list', None):
                    cpid = cpid[3:]
                if random_post.id in chat_sp + global_sp or cpid in chat_sp + global_sp:
                    continue
                if not (allow_selfposts and allow_nsfw and allow_spoilers):
                    is_self = random_post.is_self
                    nsfw = random_post.over_18
                    spoilers = random_post.spoiler
                    if cpid and not cpp:
                        cpp = await reddit.submission(cpid)
                    if cpid:
                        if not allow_selfposts:
                            is_self = cpp.is_self
                        if not (nsfw and allow_nsfw):
                            nsfw = cpp.over_18
                        if not (spoilers and allow_spoilers):
                            nsfw = cpp.spoiler
                    if is_self and not allow_selfposts:
                        continue
                    if nsfw and not allow_nsfw:
                        continue
                    if spoilers and not allow_spoilers:
                        continue
                chat_sp.append(cpid or random_post.id)
                print(random_post.id, random_post.shortlink)
                return random_post, cpp

        @aiocron.crontab(cron_duration)
        async def start_post():
            while True:
                unique_id = time.time()
                try:
                    random_post, cpp = await asyncio.wait_for(_get_submission(unique_id), timeout)
                except asyncio.TimeoutError:
                    give_ups.add(unique_id)
                    logging.error('%s timed out', chat)
                    break
                except BaseException:
                    give_ups.add(unique_id)
                    logging.exception(chat)
                else:
                    try:
                        await _actual_start_post(random_post, [chat], cpp, show_nsfw_warning, show_spoilers_warning)
                    except BaseException:
                        logging.exception(f'{random_post.id}\n{traceback.format_exc()}')
                    else:
                        break
            await write_seen_posts()

        _added_chats.append(start_post)

    for chat in send_to_chats:
        print(chat, send_to_chats[chat])
        await add_chat(chat, send_to_chats[chat])

    async def _start_broadcast(text, file, chats):
        for chat in chats:
            for i in chunks(zip_longest(text, file or []), 10):
                j, k = zip(*i)
                if not any(k):
                    k = None
                if not k and len(j) == 1:
                    j = j[0]
                if is_list_like(j) and is_list_like(k):
                    if len(j) == 1 and len(k) == 1:
                        j = j[0]
                        k = k[0]
                attributes = []
                try:
                    mimetype = (await _get_file_mimetype(k)) if k else ''
                except TypeError:
                    # (for now) telethon doesn't easily support attributes for grouped media
                    mimetype = ''
                thumb = None
                if mimetype.startswith('video/'):
                    try:
                        data = await _get_video_data(k)
                        duration = int(Decimal(data['format']['duration']))
                        w = h = None
                        for l in data['streams']:
                            if l['codec_type'] != 'video':
                                continue
                            w = l['width']
                            h = l['height']
                            break
                    except Exception:
                        logging.exception('Exception when getting video data')
                    else:
                        attributes.append(DocumentAttributeVideo(duration, w, h, supports_streaming=mimetype == 'video/mp4' or None))
                    dn, _ = os.path.split(k)
                    try:
                        nthumb = os.path.join(dn, f'{time.time()}.jpg')
                        if await _make_thumbnail(nthumb, k):
                            thumb = nthumb
                    except Exception:
                        logging.exception('Exception while making thumbnail')
                await client.send_message(chat, j, file=k, link_preview=False, attributes=attributes, thumb=thumb)

    async def _get_video_data(filename):
        proc = await asyncio.create_subprocess_exec('ffprobe', '-show_format', '-show_streams', '-print_format', 'json', filename, stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        data = json.loads(stdout)
        if data.get('format') and 'duration' not in data['format']:
            with tempfile.NamedTemporaryFile() as tempf:
                proc = await asyncio.create_subprocess_exec('ffmpeg', '-an', '-sn', '-i', filename, '-c', 'copy', '-f', 'matroska', tempf.name)
                await proc.communicate()
                ndata = await _get_video_data(tempf.name)
                if ndata.get('format') and 'duration' in ndata['format']:
                    data['format']['duration'] = ndata['format']['duration']
        return data

    async def _make_thumbnail(filename, video):
        data = await _get_video_data(video)
        if not data.get('format'):
            return False
        if data['format'].get('duration') is None:
            return False
        for i in (0, 5, 10, 15):
            if i and data['format']['duration'] > i:
                continue
            proc = await asyncio.create_subprocess_exec('ffmpeg', '-an', '-sn', '-ss', str(i), '-i', video, '-frames:v', '1', filename)
            await proc.communicate()
            if not proc.returncode:
                return True
        return False

    async def _download_file(filename, url):
        print(url)
        async with session.get(url) as resp:
            with open(filename, 'wb') as file:
                while True:
                    chunk = await resp.content.read(10)
                    if not chunk:
                        break
                    file.write(chunk)

    async def _get_file_mimetype(filename):
        mimetype = mimetypes.guess_type(filename, strict=False)[0]
        if not mimetype:
            proc = await asyncio.create_subprocess_exec('file', '--brief', '--mime-type', filename, stdout=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            mimetype = stdout.decode().strip()
        return mimetype or ''

    async def _get_file_ext(filename):
        proc = await asyncio.create_subprocess_exec('file', '--brief', '--extension', filename, stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        ext = stdout.decode().strip().split('/', maxsplit=1)[0]
        if not ext or ext == '???':
            mimetype = await _get_file_mimetype(filename)
            ext = mimetypes.guess_extension(mimetype, strict=False) or '.bin'
        if not ext.startswith('.'):
            ext = '.' + ext
        return ext

    async def _actual_start_post(random_post, chats, cpp=None, snw=None, ssw=None):
        text = f'<a href="{random_post.shortlink}">{html.escape(random_post.title)}</a>'
        nsfw = random_post.over_18
        spoilers = random_post.spoiler
        cpid = getattr(random_post, 'crosspost_parent', None)
        if cpid and getattr(random_post, 'crosspost_parent_list', None) and not cpp:
            cpp = await reddit.submission(cpid[3:])
        if cpp:
            random_post = cpp
            if snw and not nsfw:
                nsfw = random_post.over_18
            if ssw and not spoilers:
                spoilers = random_post.spoiler
            text += f' (crosspost of <a href="{random_post.shortlink}">{html.escape(random_post.title)}</a>)'
        if spoilers and ssw:
            text = '🙈🙈🙈 SPOILERS 🙈🙈🙈\n' + text
        if nsfw and snw:
            text = '🔞🔞🔞 18+ / NSFW 🔞🔞🔞\n' + text
        if not random_post.is_self:
            with tempfile.TemporaryDirectory() as tempdir:
                url = random_post.url
                filename = os.path.join(tempdir, str(time.time()))
                files = [filename]
                captions = [text]
                if random_post.is_video:
                    ffmpeg_exists = any(True for i in os.environ.get('PATH', '').split(':') if os.path.exists(os.path.join(i, 'ffmpeg')))
                    reddit_video = random_post.secure_media['reddit_video']
                    for i in ('hls_url', 'dash_url'):
                        if not ffmpeg_exists:
                            continue
                        if not reddit_video.get(i):
                            continue
                        url = reddit_video[i]
                        print(url)
                        proc = await asyncio.create_subprocess_exec('ffmpeg', '-nostdin', '-y', '-i', url, '-c', 'copy', '-f', 'mp4', filename)
                        await proc.communicate()
                        if not proc.returncode:
                            url = None
                            break
                    else:
                        if 'fallback_url' in reddit_video:
                            url = reddit_video['fallback_url']
                elif getattr(random_post, 'is_gallery', None):
                    files = []
                    captions = []
                    if getattr(random_post, 'gallery_data', None):
                        gallery_keys = map(lambda i: i[1], sorted(map(lambda i: (i['id'], i['media_id']), random_post.gallery_data['items']), key=lambda i: i[0]))
                    else:
                        gallery_keys = random_post.media_metadata.keys()
                    for a, i in enumerate(gallery_keys):
                        i = random_post.media_metadata[i]
                        if i['status'] == 'valid':
                            filename = os.path.join(tempdir, str(time.time()))
                            for j in ('u', 'mp4', 'gif'):
                                if j in i['s']:
                                    await _download_file(filename, i['s'][j])
                                    break
                            captions.append(f'{text}\n#{a + 1}')
                            files.append(filename)
                    url = None
                if url:
                    parsed = list(urlparse(url))
                    splitted = os.path.splitext(parsed[2])
                    domain = getattr(random_post, 'domain', parsed[1])
                    preview = getattr(random_post, 'preview', None)
                    if domain == 'imgur.com' or domain.endswith('.imgur.com'):
                        parsed[1] = 'i.imgur.com'
                        if parsed[2].startswith('/a/') or parsed[2].startswith('/gallery/'):
                            albumid = os.path.split(parsed[2].rstrip('/'))[1]
                            async with session.get(f'https://imgur.com/ajaxalbums/getimages/{albumid}/hit.json?all=true') as resp:
                                apidata = (await resp.json())['data']
                            if apidata['count'] == 1:
                                parsed[2] = apidata['images'][0]['hash'] + apidata['images'][0]['ext']
                                desc = apidata['images'][0]['description']
                                if desc:
                                    captions[0] += '\n' + html.escape(desc)
                            else:
                                files = []
                                captions = []
                                for a, i in enumerate(apidata['images']):
                                    to_append = f'#{a + 1}'
                                    desc = i['description']
                                    if desc:
                                        to_append += ': ' + desc.strip()
                                    caplength = 1023 - len(client.parse_mode.parse(text)[0])
                                    captext = to_append[:caplength]
                                    if len(captext) >= caplength:
                                        captext = captext[:-1]
                                        captext += '…'
                                    captions.append(text + '\n' + html.escape(captext))
                                    filename = os.path.join(tempdir, str(time.time()))
                                    await _download_file(filename, f'https://i.imgur.com/{i["hash"]}{i["ext"]}')
                                    files.append(filename)
                                url = None
                        if splitted[1] == '.gifv':
                            parsed[2] = splitted[0] + '.mp4'
                        if url:
                            url = urlunparse(parsed)
                    elif domain == 'gfycat.com':
                        async with session.get(f'https://api.gfycat.com/v1/gfycats/{splitted[0]}') as resp:
                            apidata = await resp.json()
                        gfyitem = apidata.get('gfyItem')
                        if gfyitem:
                            url = gfyitem.get('mp4Url', url)
                    elif random_post.is_reddit_media_domain and preview:
                        ppreview = preview['images'][0]
                        if splitted[1] == '.gif':
                            for i in ('mp4', 'gif'):
                                if i in ppreview['variants']:
                                    url = ppreview['variants'][i]['source']['url']
                                    break
                        elif random_post.is_video:
                            url = ppreview['source']['url']
                if url:
                    url = urlunparse(urlparse(url, 'https'))
                    await _download_file(filename, url)
                    mimetype = await _get_file_mimetype(filename)
                    if mimetype.startswith('image') and preview and preview.get('enabled'):
                        preview = preview['images'][0]
                        urls = [i['url'] for i in preview['resolutions']]
                        urls.append(preview['source']['url'])
                        urls.reverse()
                        for url in urls:
                            if os.path.getsize(filename) < 10000000:
                                break
                            url = urlunparse(urlparse(url, 'https'))
                            await _download_file(filename, url)
                    ext = await _get_file_ext(filename)
                    if ext in ('.htm', '.html'):
                        with open(filename) as file:
                            soup = BeautifulSoup(file.read())
                        ptitle = soup.find(lambda tag: tag.name == 'meta' and tag.attrs.get('property') == 'og:title' and tag.attrs.get('content')) or soup.find('title')
                        if ptitle:
                            ptitle = ptitle.attrs.get('content', ptitle.text).strip()
                        pdesc = soup.find(lambda tag: tag.name == 'meta' and tag.attrs.get('property') == 'og:description' and tag.attrs.get('content')) or soup.find(lambda tag: tag.name == 'meta' and tag.attrs.get('name') == 'description' and tag.attrs.get('content'))
                        if pdesc:
                            pdesc = pdesc.attrs.get('content', pdesc.text).strip()
                        pmedia = soup.find(lambda tag: tag.name == 'meta' and tag.attrs.get('property') == 'og:video' and tag.attrs.get('content')) or soup.find(lambda tag: tag.name == 'meta' and tag.attrs.get('property') == 'og:image' and tag.attrs.get('content'))
                        if pmedia:
                            pmedia = pmedia.attrs.get('content', '').strip()
                        tat = f'{text}\n\nURL: '
                        if ptitle:
                            tat += f'<a href="{url}">{html.escape(ptitle)}</a>'
                        else:
                            tat += url
                        files = []
                        if pmedia:
                            pmedia = urljoin(url, pmedia)
                            await _download_file(filename, pmedia)
                            if await _get_file_mimetype(filename) == 'video/x-m4v':
                                ofilename = filename + '.oc'
                                os.rename(filename, ofilename)
                                proc = await asyncio.create_subprocess_exec('ffmpeg', '-nostdin', '-y', '-i', ofilename, '-c', 'copy', '-f', 'mp4', filename)
                                await proc.communicate()
                                if not proc.returncode:
                                    os.remove(ofilename)
                                else:
                                    os.rename(ofilename, filename)
                                    try:
                                        os.remove(filename)
                                    except FileNotFoundError:
                                        pass
                            files.append(filename)
                        if pdesc:
                            caplength = 1023 if pmedia else 4095
                            caplength -= len(client.parse_mode.parse(tat)[0])
                            captext = pdesc[:caplength]
                            if len(captext) >= caplength:
                                captext = captext[:-1]
                                captext += '…'
                            tat += '\n' + captext
                        captions = [tat]
                for a, i in enumerate(files):
                    ext = await _get_file_ext(i)
                    os.rename(i, i + ext)
                    files[a] = i + ext
                await _start_broadcast(captions, files, chats)
        else:
            if getattr(random_post, 'selftext', None):
                caplength = 4094 - len(client.parse_mode.parse(text)[0])
                text += '\n\n'
                captext = random_post.selftext.strip()[:caplength]
                if len(captext) >= caplength:
                    captext = captext[:-1]
                    captext += '…'
                text += html.escape(captext)
            await _start_broadcast([text], None, chats)

    def register(pattern):
        def wrapper(func):
            @functools.wraps(func)
            @client.on(events.NewMessage(chats=bot_admins, pattern=pattern))
            async def awrapper(e):
                try:
                    await func(e)
                except BaseException:
                    await e.reply(traceback.format_exc(), parse_mode=None)
                    raise
            return awrapper
        return wrapper

# change this as you please. 
    @register('/(start|help)')
    async def start_or_help(e):
        await e.reply(('hmm?'), parse_mode=None)

    @register('/poweroff')
    async def poweroff(e):
        await e.reply('ok')
        await e.client.disconnect()

    @register(r'/test (\S+)(?: ([ns]+))?')
    async def test_post(e):
        await e.reply('ok')
        post = await reddit.submission(e.pattern_match.group(1))
        flags = e.pattern_match.group(2) or ''
        snw = 'n' in flags
        ssw = 's' in flags
        await _actual_start_post(post, [e.chat_id], None, snw, ssw)

#    await asyncio.gather(*[i.func() for i in _added_chats])
    try:
        await client.run_until_disconnected()
    finally:
        await session.close()

if __name__ == '__main__':
    asyncio.run(main())
