import requests
import socket
import time
import atexit
import threading
import json
import logging
import re
import itertools
import isodate
import random
from datetime import datetime
import pytz


def is_json(string):
    try:
        json.loads(string)
        return True
    except ValueError:
        return False


def parse_int(val):
    try:
        return int(val)
    except ValueError:
        return -1


def parse_duration(iso):
    return isodate.parse_duration(iso).seconds if iso else 0


def print_duration_iso(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    days, hours, minutes = map(int, (days, hours, minutes))
    seconds = round(seconds, 6)
    result = 'P'
    if days:
        result += str(days) + 'D'
    result += 'T'
    if hours:
        result += str(hours) + 'H'
    if minutes:
        result += str(minutes) + 'M'
    if seconds or result == 'PT':
        result += str(int(seconds)) + 'S'
    return result


def print_duration(seconds):
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    return ((str(int(h)) + ' hour' + ('s' if h != 1 else '') + ', ' if h > 0 else '') +
            (str(int(m)) + ' minute' + ('s' if m != 1 else '') + ', ' if m > 0 else '') +
            (str(int(s)) + ' second' + ('s' if s != 1 else '') + ', ' if s > 0 >= h else '')
            )[:-2]


def url_encode(val):
    return val.replace('#', '%23').replace('&', '%26')


def api(request, endpoint, headers=None, params=None, data=None):
    if headers is None:
        headers = {}
    if 'Authorization' not in headers:
        headers['Authorization'] = oauth['Authorization']
    headers['Client-ID'] = 'UQqmoQbkoTqQRensiMDenSRtOFliJW'
    uri = 'http' + ('' if prod else 's') + '://api.nerdbot.tv/5/' + endpoint
    return request(uri, headers=headers, params=params, data=data)


def twitch_api(request, endpoint, headers=None, params=None, data=None):
    if headers is None:
        headers = {}
    headers['Accept'] = 'application/vnd.twitchtv.v5+json'
    headers['Client-ID'] = 'je4sa1f9posdx0wti0kf07xovm6fzuw'
    uri = 'https://api.twitch.tv/kraken/' + endpoint
    return request(uri, headers=headers, params=params, data=data)


def clean_up():
    print('Cleaning up')
    keys = bots.copy().keys()
    for key in keys:
        bots[key].clean_up()


def join_channel(user_info):
    if user_info['twitch_login'] is None:
        user_info['twitch_login'] = twitch_login
    if user_info['twitch_login']['name'].lower() in bots.keys():
        bot = bots[user_info['twitch_login']['name'].lower()]
    else:
        bot = Bot(user_info['twitch_login']['name'], user_info['twitch_login']['token'])
    bot.join_channel(user_info)


class Bot(threading.Thread):
    def __init__(self, name, token, init=True):
        if init:
            super().__init__()
            if name.lower() in bots.keys():
                return
        bots[name.lower()] = self
        if not hasattr(self, 'channels'):
            self.channels = {}
        self.name = name
        self.token = token
        self.readQueue = ''
        self.sendQueue = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.socket.connect(('irc.chat.twitch.tv', 6667))
            self.socket.setblocking(False)
        except Exception as e:
            print(e)
            print('Failed to connect: ' + name)
            time.sleep(5)
            self.reset()
            return
        self.socket.send('CAP REQ :twitch.tv/tags\r\n'.encode('utf-8'))
        self.socket.send(('PASS oauth:' + token + '\r\n').encode('utf-8'))
        self.socket.send(('NICK ' + name + '\r\n').encode('utf-8'))
        self.alive = True
        self.stop = False
        self.ready = False
        if init:
            self.daemon = True
            self.start()

    def run(self):
        global line_queue
        while not self.stop:
            if self.alive:
                try:
                    try:
                        incoming = self.socket.recv(4096)
                        self.readQueue = self.readQueue + incoming.decode('utf-8')
                        read = self.readQueue.split('\n')
                        self.readQueue = read.pop()
                        for line in read:
                            words = str.rstrip(line).split()
                            print(words)
                            if len(words) == 5 and words[1] == '001':
                                print('Signed in: ' + self.name)
                                self.ready = True
                                for channel in self.channels.keys():
                                    self.sendQueue.append('JOIN #' + channel)
                            elif len(words) == 2 and words[0] == 'PING':
                                self.send('PONG ' + words[1])
                            elif len(words) == 6 and words[3] == ':Error':
                                return
                            elif len(words) > 3 and (words[1] == 'PRIVMSG' or words[2] == 'PRIVMSG'):
                                print('PRIVMSG')
                                for channel in self.channels.values():
                                    if channel.name.lower() == words[3 if words[2] == 'PRIVMSG' else 2][1:]:
                                        line_queue.append([self.name.lower(), channel.name.lower(), words])
                    except BlockingIOError:
                        pass
                    if self.ready:
                        send = self.sendQueue[:]
                        for line in send:
                            line = line.replace('\n', '\\n')
                            self.socket.send((line + '\r\n').encode('utf-8'))
                            del self.sendQueue[0]
                except IOError:
                    print('IO Error: ' + self.name)
                    time.sleep(1)
                    self.reset()

    def join_channel(self, user_info):
        name = user_info['sites']['twitch']['name']
        print('Joining: ' + name)
        if name.lower() not in self.channels.keys():
            self.channels[name.lower()] = Channel(user_info)
        if self.ready:
            self.sendQueue.append('JOIN #' + name.lower())

    def leave_channel(self, name):
        print('Leaving channel: ' + name)
        if name.lower() in self.channels.keys():
            self.channels[name.lower()].clean_up()
            self.channels.pop(name.lower())
        if self.ready:
            self.sendQueue.append('PART #' + name.lower())

    def reset(self):
        print('Resetting bot: ' + self.name)
        self.alive = False
        self.ready = False
        self.socket.close()
        self.__init__(self.name, self.token, False)

    def clean_up(self):
        print('Cleaning up bot: ' + self.name)
        self.stop = True
        self.socket.close()
        bots.pop(self.name.lower())

    def send(self, line):
        self.sendQueue.append(line)


class Channel:
    def __init__(self, user_info):
        super().__init__()
        self.id = str(user_info['id'])
        self.raffle_answer = None
        self.raffle_entries = None
        self.raffle_max_entries = None
        self.raffle_tickets = None
        self.raffle_cost = None
        self.last_timer = 0
        self.last_promo = 0
        self.lines_since_timer = 0
        self.lines_since_promo = 0
        self.command_uses = {}
        self.name = user_info['sites']['twitch']['name']
        self.user = user_info
        self.stop = False
        self.commands = {}
        self.regulars = []
        self.permits = []
        self.logs = []
        self.warnings = {}
        self.banned_words = []
        self.allowed_links = []
        self.last_follower_check = 0
        self.timers = {'default': [], 'promo': []}
        self.last_payout = 0
        if self.user['twitch_login']['name'].lower() in bots:
            self.bot = bots[self.user['twitch_login']['name'].lower()]
        else:
            print('No bot found for channel: ' + self.name)
            return
        self.update('commands', True)
        self.update('regulars', True)
        self.update('timers', True)
        """
        banned_words = api(requests.get, 'users/' + self.id + '/banned-words', oauth)
        if banned_words.status_code == 200:
            for word in banned_words.json():
                self.banned_words.append(word['text'])
        allowed_links = api(requests.get, 'users/' + self.id + '/allowed-links', oauth)
        if allowed_links.status_code == 200:
            for whiteLink in allowed_links.json():
                self.allowed_links.append(whiteLink['text'])"""
        self.join_time = time.time()

    def update(self, part, init=False):
        if not init:
            print('Updating ' + part + ': ' + self.name)
        if part == 'settings':
            user_info = api(requests.get, 'users/' + self.id)
            if user_info.status_code == 200:
                self.user = user_info.json()
        if part == 'commands':
            commands = api(requests.get, 'users/' + self.id + '/commands')
            if commands.status_code == 200:
                self.commands.clear()
                for command in commands.json():
                    self.commands[command['name']] = command
        if part == 'regulars':
            regulars = api(requests.get, 'users/' + self.id + '/regulars')
            if regulars.status_code == 200:
                self.regulars = regulars.json()
        if part == 'timers':
            timers = api(requests.get, 'users/' + self.id + '/timers')
            if timers.status_code == 200:
                self.timers = {'default': [], 'promo': []}
                for timer in timers.json():
                    self.timers[timer['set']].append(timer)

    def main(self):
        if time.time() - self.last_follower_check >= 300:
            self.last_follower_check = time.time()
            if self.user['alerts']['follower'] is not None:
                followers = twitch_api(requests.get, 'channels/' + self.name + '/follows', params={'limit': 5})
                if followers.status_code == 200:
                    notify = []
                    for follower in followers.json()['follows']:
                        name = follower['user']['name']
                        follow = api(requests.get, 'users/' + self.name + '/followers/' + name)
                        if follower.status_code == 404:
                            api(requests.put, 'users/' + self.name + '/followers/' + name)
                            notify.append(follow['user']['display_name'])
                    if len(notify) > 0:
                        line = self.user['alerts']['follower'].replace('{names}', ', '.join(notify))
                        self.send(self.parse_vars(line, None, None, None))
        for item in self.logs:
            api(requests.post, 'users/' + self.name + '/logs', data={'text': item})
        self.logs = []
        interval = self.user['timers']['sets']['default']['interval']
        lines_passed = self.lines_since_timer >= self.user['timers']['min_lines']
        if interval and time.time() - self.last_timer >= parse_duration(interval) and lines_passed:
            timers = self.timers['default']
            if len(timers) > 0:
                self.last_timer = self.last_timer + 1 if len(timers) - 1 > self.last_timer else 0
                message = self.parse_vars(timers[self.last_timer]['message'], None, None, None)
                if message.startswith('/') and not message.lower().startswith('/me '):
                    message = '\xE2\x80\x8B' + message
                self.send(message[:497] + '...' if len(message) > 500 else message)
                self.lines_since_timer = 0
            self.last_timer = time.time()
        interval = self.user['timers']['sets']['promo']['interval']
        lines_passed = self.lines_since_promo >= self.user['timers']['min_lines']
        if interval and time.time() - self.last_promo >= parse_duration(interval) and lines_passed:
            timers = self.timers['promo']
            if len(timers) > 0:
                self.last_promo = self.last_promo + 1 if len(timers) - 1 > self.last_promo else 0
                message = self.parse_vars(timers[self.last_promo]['message'], None, None, None)
                if message.startswith('/') and not message.lower().startswith('/me '):
                    message = '\xE2\x80\x8B' + message
                self.send(message[:497] + '...' if len(message) > 500 else message)
                self.lines_since_promo = 0
            self.last_promo = time.time()
        if self.user['points']['payout_interval']:
            amount = self.user['points']['payout_amount']
            sec = parse_duration(self.user['points']['payout_interval'])
            if amount > 0 and 0 < sec <= time.time() - self.last_payout and self.user['points']['enabled']:
                self.last_payout = time.time()
                stream = twitch_api(requests.get, 'streams/' + self.name)
                if stream.status_code == 200 and stream.json()['stream']:
                    self.add_to_all(amount, sec)

    def clean_up(self):
        print('Cleaning up channel: ' + self.name)
        self.stop = True

    def is_allowed(self, sender, allowed, ignore_mods=False):
        if sender.admin or sender.level == 'owner':
            return True
        if sender.level == 'mod' and ('moderators' in allowed or ignore_mods):
            return True
        is_regular = str(sender.twitch_id) in self.regulars
        if 'regulars' in allowed and is_regular:
            return True
        if 'subscribers' in allowed and sender.sub:
            return True
        if 'viewers' in allowed and sender.level == 'reg' and not is_regular and not sender.sub:
            return True
        return False

    def parse_vars(self, line, sender, command, args):
        if command:
            line = line.replace('{uses}', '{:,}'.format(command['uses']))
            line = line.replace('{cooldown}', str(parse_duration(command['cooldown'])))
        if args:
            for i in range(1, 10):
                line = line.replace('{' + str(i) + '}', '' if len(args) < i else args[i - 1])
            line = line.replace('{*}', ' '.join(args))
        for twitch in re.findall('\{twitch ([^}]+)}', line):
            if ('{twitch ' + twitch + '}') in line:
                params = twitch.split(' ')
                if len(params) < 2:
                    continue
                replace = None
                twitch_user = params[0]
                if twitch_user == 'sender':
                    if not sender:
                        continue
                    twitch_user = sender.twitch_id
                elif twitch_user == 'channel':
                    twitch_user = self.user['sites']['twitch']['id']
                if parse_int(twitch_user) == -1:
                    user_info = twitch_api(requests.get, 'users?login=' + str(twitch_user))
                    if user_info.status_code != 200:
                        continue
                    user_info = user_info.json()
                    if user_info['_total'] != 1:
                        continue
                    twitch_user = user_info['users'][0]
                else:
                    user_info = twitch_api(requests.get, 'users/' + str(twitch_user))
                    if user_info.status_code != 200:
                        continue
                    twitch_user = user_info.json()
                if params[1] == 'name':
                    replace = twitch_user['display_name']
                elif params[1] == 'id':
                    replace = twitch_user['_id']
                elif params[1] == 'role' and sender:
                    replace = sender.level
                elif params[1] == 'type':
                    replace = twitch_user['type']
                elif params[1] == 'uptime':
                    stream = twitch_api(requests.get, 'streams/' + twitch_user['_id'])
                    if stream.status_code != 200:
                        replace = '(failed to get)'
                    else:
                        stream = stream.json()
                        if stream['stream'] is None:
                            replace = '(offline)'
                        else:
                            started = isodate.parse_datetime(stream['stream']['created_at'])
                            replace = print_duration((datetime.now(pytz.utc) - started).total_seconds())
                if replace:
                    line = line.replace('{twitch ' + ' '.join(params) + '}', replace)
        for url in re.findall('\{api ([^}]+)}', line):
            if ('{api ' + url + '}') in line:
                target_url = (url.replace('https://api.rtainc.co', 'http://api.rtainc.co')
                                 .replace('https://apis.rtainc.co', 'http://apis.rtainc.co'))
                line = line.replace('{api ' + url + '}', requests.get(target_url).text)
        return line

    def timeout(self, sender, cause):
        name = sender.name.lower()
        warning = name not in self.warnings.keys() or time.time() - self.warnings[name] >= 600
        self.warnings[name] = time.time()
        action = 'Warned' if warning else 'Timed out'
        tag = ' (Warning)' if warning else ''
        self.logs.append(action + ' <span>' + sender.name + '</span> - <span>' + cause + '</span>')
        if self.user['anti_spam']['notice_type'] == 'chat':
            self.send(cause + ' (' + sender.name + ')' + tag)
        self.send('/timeout ' + name + ' ' + ('10' if warning else '600'))
        time.sleep(1)
        self.send('/timeout ' + name + ' ' + ('10' if warning else '600'))

    def get_viewer(self, name):
        points = api(requests.get, 'users/' + self.id + '/viewers/' + name)
        return points.json() if points.status_code == 200 else None

    def add_to_all(self, points, seconds=0):
        result = requests.get('http://tmi.twitch.tv/group/user/' + self.name.lower() + '/chatters')
        if result.status_code == 200:
            result = result.json()
            chatters = []
            chatters.extend(result['chatters']['moderators'])
            chatters.extend(result['chatters']['staff'])
            chatters.extend(result['chatters']['admins'])
            chatters.extend(result['chatters']['global_mods'])
            chatters.extend(result['chatters']['viewers'])
            for chatter in chatters:
                viewer = api(requests.get, 'users/' + self.id + '/viewers/' + chatter)
                if viewer.status_code == 404:
                    data = {
                        'points': points,
                        'time_watched': 'PT' + str(seconds) + 'S'
                    }
                    api(requests.put, 'users/' + self.id + '/viewers/' + chatter, data=data)
                elif viewer.status_code == 200:
                    viewer = viewer.json()
                    data = {
                        'points': viewer['points'] + points,
                        'time_watched': 'PT' + str(parse_duration(viewer['time_watched']) + seconds) + 'S'
                    }
                    api(requests.patch, 'users/' + self.id + '/viewers/' + chatter, data=data)

    def add_to_viewer(self, name, points=None, seconds=None):
        viewer = api(requests.get, 'users/' + self.id + '/viewers/' + name)
        data = {}
        if viewer.status_code == 404:
            if points is not None:
                data['points'] = points
            if seconds is not None:
                data['time_watched'] = 'PT' + str(seconds) + 'S'
            api(requests.put, 'users/' + self.id + '/viewers/' + name, data=data)
        elif viewer.status_code == 200:
            viewer = viewer.json()
            if points is not None:
                data['points'] = viewer['points'] + points
            if seconds is not None:
                data['time_watched'] = 'PT' + str(parse_duration(viewer['time_watched']) + seconds) + 'S'
            api(requests.patch, 'users/' + self.id + '/viewers/' + name, data=data)

    def set_viewer(self, name, points=None, seconds=None):
        viewer = api(requests.get, 'users/' + self.id + '/viewers/' + name)
        request = requests.patch if viewer.status_code == 200 else requests.put
        data = {}
        if points is not None:
            data['points'] = points
        if seconds is not None:
            data['timeWatched'] = 'PT' + str(seconds) + 'S'
        api(request, 'users/' + self.id + '/viewers/' + name, data=data)

    def on_line(self, words):
        tags = {}
        if words[0][0] == '@':
            for tag in words[0][1:].split(';'):
                tags[tag[:tag.index('=')]] = tag[tag.index('=') + 1:]
            words = words[1:]
        sender_name = words[0][1:words[0].index('!')]
        sender_id = parse_int(tags['user-id']) if 'user-id' in tags else None
        sender = Sender(sender_id, sender_name, 'reg', False, sender_id in admins)
        if sender.name == words[2][1:] or sender_id in admins:
            sender.level = 'owner'
        elif 'user-type' in tags.keys() and tags['user-type'] in ['mod', 'global_mod', 'admin', 'staff']:
            sender.level = 'mod'
        if 'subscriber' in tags.keys() and tags['subscriber'] == '1':
            sender.sub = True
        if 'display-name' in tags.keys() and tags['display-name'] != '':
            sender.name = tags['display-name'].replace('\\s', '')
        line = ' '.join(words[3:])[1:]
        line_lower = line.lower()
        tickets = parse_int(line_lower[line_lower.rfind(' ') + 1:])
        if tickets > 0:
            line_lower = line_lower[:line_lower.rfind(' ')]
        else:
            tickets = 1
        if self.raffle_answer and line_lower == self.raffle_answer.lower():
            if self.raffle_entries.count(sender.name.lower()) + tickets > self.raffle_max_entries:
                tickets_left = self.raffle_max_entries - self.raffle_entries.count(sender.name.lower())
                return self.send(sender.name + ' -> You may only get {:,} more tickets.'.format(tickets_left))
            if self.raffle_cost > 0:
                cost = self.raffle_cost * tickets
                points = self.get_viewer(sender.name)
                points = points['points'] if points else 0
                if points < cost:
                    return self.send(sender.name + ' -> You need {:,} more points.'.format(cost - points))
                params = {'points': str(points - cost)}
                api(requests.patch, 'users/' + self.id + '/viewers/' + sender.name, params=params)
            for _ in itertools.repeat(None, tickets):
                self.raffle_entries.append(sender.name.lower())
        emote_count = 0
        for word in words:
            if word in emotes:
                emote_count += 1
        enforce = (sender.name.lower() != 'jtv' and sender.level == 'reg' and
                   (not sender.sub or not self.user['anti_spam']['ignore_subs']))
        self.lines_since_timer += 1
        self.lines_since_promo += 1
        if sender.name.lower() == 'twitchnotify':
            if 'subscribed' in line and 'while you were away' not in line and ' to ' not in line:
                sub = line[:line.index(' ')]
                months = 1 if 'months in a row' not in line else int(line.split(' ')[3])
                if months == 1 and self.user['alerts']['subscriber']:
                    self.logs.append('<span>' + sub + '</span> subscribed for the first time.')
                    self.send(self.user['alerts']['subscriber'].replace('{name}', sub).replace('{months}', '1'))
                elif months != 1 and self.user['alerts']['resubscriber']:
                    log = '<span>{}</span> subscribed for <span>{} months</span> in a row.'.format(sub, months)
                    alert = self.user['alerts']['resubscriber'].replace('{name}', sub).replace('{months}', str(months))
                    self.logs.append(log)
                    self.send(alert)
            return
        songs = self.is_allowed(sender, self.user['song_requests']['allow_from'])
        has = re.findall(link, line, re.I)
        if len(has) > 0 and (not songs or words[3][1:].lower() != '!songrequest'):
            match = re.sub(r'[^A-Za-z0-9\.]+', '', has[0][0].lower().replace('dot', '.').replace('d0t', '.'))
            if match not in self.allowed_links:
                links = self.user['anti_spam']['allow_links_from']
                if sender.name.lower() in self.permits or self.is_allowed(sender, links, True):
                    if sender.name.lower() in self.permits:
                        self.permits.remove(sender.name.lower())
                else:
                    for_name = 'subs' if links == 1 else ('regulars' if links == 2 else 'mods')
                    return self.timeout(sender, 'Only ' + for_name + ' can post links.')
        test = re.sub(r'([^a-z]|^)([a-z]{1,3})([^a-z]|$)', '', re.sub(r'[^A-Za-z]+', '', line))
        regex = '[A-Z]{{},}'.replace('{}', str(self.user['anti_spam']['max_caps']))
        if enforce and self.user['anti_spam']['max_caps'] > 0 and re.search(regex, test):
            return self.timeout(sender, 'Too many caps.')
        if enforce and emote_count > self.user['anti_spam']['max_emotes'] > 0:
            return self.timeout(sender, 'Too many emotes.')
        if enforce:
            for text in self.banned_words:
                regex = '\\b' + re.escape(text.lower().replace('*', 'WC')).replace('WC', '\\S*') + '\\b'
                if re.search(regex, line, re.I):
                    return self.timeout(sender, 'No using banned words.')
        if len(words) > 3 and len(words[3]) > 1:
            try:
                response = self.on_command(sender, words[3][1:].lower(), words[4:], False)
            except BaseException as e:
                response = 'An error occurred: ' + str(e)
                logging.exception('Failed to handle chat line in channel ' + self.name + ':')
            if response is not None:
                self.send(sender.name + ' -> ' + response)

    def on_command(self, sender, command, args, is_alias):
        if command == '!nerdbot' and sender.level != 'reg':
            if len(args) == 0:
                return 'I\'m here!'
            if args[0] == 'debug' and sender.admin:
                return json.dumps({
                    'time_since_start': print_duration_iso(time.time() - start_time),
                    'version': version,
                    'is_plus': self.user['plus']['active'],
                    'time_since_join': print_duration_iso(time.time() - self.join_time),
                    'command_count': len(self.commands)
                })
            if args[0] == 'leave':
                api(requests.patch, '/users/' + self.id, data={'mode': 'off' if prod else 'beta/off'})
                self.bot.leave_channel(self.name)
                return 'I\'m leaving the chat. You can get me back on the dashboard.'
            if args[0] == 'sendraw' and sender.admin and len(args) > 1:
                self.bot.send(' '.join(args[1:]))
                return 'Line sent to chat via ' + self.bot.name + '.'
        elif command == '!setcom' and sender.level != 'reg' and len(args) > 1:
            name = args[0]
            exists = api(requests.get, 'users/' + self.id + '/commands/' + name).status_code != 404
            allow_for = None
            cooldown = None
            uses = None
            response = ' '.join(args[1:])
            if not any(name.startswith(prefix) for prefix in prefixes):
                return 'Command name must start with a symbol.'
            for i in range(1, min(3, len(args))):
                if args[i].startswith('-for='):
                    response = ' '.join(response.split(' ')[1:])
                    allow_for = args[i][5:].split(',')
                    if args[i] == '-for=':
                        return 'At least one group of users must be allowed.'
                    if any(allow not in user_roles for allow in allow_for):
                        return 'Users only include viewers, subs, regulars, and mods.'
                if args[i].startswith('-cooldown='):
                    response = ' '.join(response.split(' ')[1:])
                    cooldown = parse_int(args[i][10:])
                    if cooldown < 0:
                        return 'Cooldown must be a positive number of seconds.'
                    cooldown = "PT" + str(cooldown) + "S"
                if args[i].startswith('-uses='):
                    response = ' '.join(response.split(' ')[1:])
                    uses = parse_int(args[i][6:])
                    if uses < 0:
                        return 'Use count must be positive.'
            if exists:
                data = {'cooldown': cooldown, 'allow_for': allow_for, 'uses': uses, 'response': response}
                for key, value in data.copy().items():
                    if value is None:
                        data.pop(key)
            else:
                data = {'cooldown': cooldown, 'allow_for': allow_for, 'uses': uses, 'response': response}
                if data['cooldown'] is None:
                    data['cooldown'] = 0
                if data['allow_for'] is None:
                    data['allow_for'] = user_roles
                if data['uses'] is None:
                    data['uses'] = 0
            request = requests.patch if exists else requests.put
            api(request, 'users/' + self.id + '/commands/' + name, data=data)
            self.update('commands')
            return ('Updated' if exists else 'Added') + ' the command ' + name + '.'
        elif command == '!delcom' and sender.level != 'reg' and len(args) == 1:
            name = args[0]
            request = api(requests.delete, 'users/' + self.id + '/commands/' + name)
            if request.status_code == 404:
                return 'There\'s no command named ' + name + '.'
            self.update('commands')
            return 'Deleted the command ' + name + '.'
        elif command == '!permit' and sender.level != 'reg' and len(args) == 1:
            name = args[0]
            self.permits.append(name.lower())
            return 'The user ' + name + ' may now post a link.'
        elif command == self.user['points']['command'].lower() and self.user['points']['enabled']:
            if len(args) == 0:
                points = self.get_viewer(sender.name)
                points = points['points'] if points else 0
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                return ('You have {:,} ' + name + '.').format(points)
            elif args[0] == 'get':
                if len(args) == 1:
                    return 'Usage: ' + self.user['points']['command'].lower() + ' get <name>.'
                points = self.get_viewer(args[1].lower())
                points = points['points'] if points else 0
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                return (args[1] + ' has {:,} ' + name + '.').format(points)
            elif args[0] == 'pay' and len(args) == 3:
                points = self.get_viewer(sender.name)
                points = points['points'] if points else 0
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                if parse_int(args[2]) < 1:
                    return 'You can\'t give negative ' + self.user['points']['name']['plural'] + '.'
                if not re.match('^[a-zA-Z0-9_]{4,25}$', args[1]):
                    return 'That username is invalid.'
                if points == 0:
                    return 'You don\'t have any {} to give.'.format(name)
                if int(args[2]) > points:
                    return 'You only have {:,} {} to give.'.format(points, name)
                if sender.name.lower() == args[1].lower():
                    return 'You can\'t give ' + self.user['points']['name']['plural'] + ' to yourself.'
                else:
                    give = parse_int(args[2])
                    name = self.user['points']['name']['singular'] if give == 1 else self.user['points']['name']['plural']
                    self.add_to_viewer(sender.name, -give)
                    self.add_to_viewer(args[1], give)
                    return ('Paid {:,} ' + name + ' to ' + args[1] + '.').format(give)
            elif args[0] == 'give' and sender.level != 'regular' and len(args) == 3:
                if not re.match('^[a-zA-Z0-9_]{4,25}$', args[1]):
                    return 'That username is invalid.'
                points = parse_int(args[2])
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                self.add_to_viewer(args[1], points)
                return 'Gave {:,} {} to {}.'.format(points, name, args[1])
            elif args[0] == 'take' and sender.level != 'regular' and len(args) == 3:
                if not re.match('^[a-zA-Z0-9_]{4,25}$', args[1]):
                    return 'That username is invalid.'
                points = parse_int(args[2])
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                self.add_to_viewer(args[1], -points)
                return 'Took {:,} {} from {}.'.format(points, name, args[1])
            elif args[0] == 'set' and sender.level != 'regular' and len(args) == 3:
                if not re.match('^[a-zA-Z0-9_]{4,25}$', args[1]):
                    return 'That username is invalid.'
                points = parse_int(args[2])
                name = self.user['points']['name']['singular'] if points == 1 else self.user['points']['name']['plural']
                self.set_viewer(args[1], points)
                return 'Set {}\'s balance to {:,} {}.'.format(args[1], points, name)
            elif args[0] == 'giveall' and sender.level != 'regular':
                if len(args) == 2 and parse_int(args[1]) > 0:
                    give = parse_int(args[1])
                    name = self.user['points']['name']['singular'] if give == 1 else self.user['points']['name']['plural']
                    self.add_to_all(give)
                    message = '<span>{}</span> gave <span>{:,} {}</span> to all viewers.'
                    self.logs.append(message.format(sender.name, give, name))
                    return 'Gave {:,} {} to everyone watching.'.format(give, name)
        elif command in self.commands.keys():
            command_info = self.commands[command]
            if not self.is_allowed(sender, command_info['allow_for']):
                return
            if command in self.command_uses.keys():
                if time.time() - self.command_uses[command] < parse_duration(command_info['cooldown']):
                    return
            self.command_uses[command] = time.time()
            command_info['uses'] += 1
            text = self.parse_vars(command_info['response'], sender, command_info, args)
            if text.startswith('/') and not text.lower().startswith('/me '):
                text = '\xE2\x80\x8B' + text
            has_alias = False
            if not is_alias:
                for match in re.findall('\{alias ([^}]+)}', text):
                    if ('{alias ' + match + '}') in text and match[0] in prefixes:
                        args = match.replace('{uses}', str(command_info['uses'])).split(' ')[1:] + args
                        redirect = self.on_command(sender, match.split(' ')[0].lower(), args, True)
                        if redirect is not None:
                            self.send(redirect[:497] + '...' if len(redirect) > 500 else redirect)
                        has_alias = True
            if not has_alias:
                self.send(text[:497] + '...' if len(text) > 500 else text)
            data = {'uses': command_info['uses']}
            api(requests.patch, 'users/' + self.id + '/commands/' + url_encode(command), data=data)

    def following_tag(self, name):
        following = twitch_api(requests.get, 'users/' + name + '/follows/channels/' + self.name)
        if following.status_code == 200:
            name += ' (following)'
        elif following.status_code == 404:
            name += ' (not following)'
        return name

    def start_raffle(self, style, args):
        sec = args[0]
        disclaimer = '/me A raffle is starting! This is a promotion from {} and is not sponsored or endorsed by Twitch.'
        self.send(disclaimer.format(self.name))
        time.sleep(1)
        if style == 'random':
            result = requests.get('http://tmi.twitch.tv/group/user/' + self.name.lower() + '/chatters')
            if result.status_code == 200:
                result = result.json()
                chatters = []
                chatters.extend(result['chatters']['moderators'])
                chatters.extend(result['chatters']['staff'])
                chatters.extend(result['chatters']['admins'])
                chatters.extend(result['chatters']['global_mods'])
                chatters.extend(result['chatters']['viewers'])
                if len(chatters) > 0:
                    self.send('/me Picking a random viewer in ' + print_duration(sec) + '...')
                    time.sleep(sec)
                    winner = self.following_tag(random.choice(chatters))
                    self.send('/me ' + winner + ' won the raffle!')
                    return winner
                self.send('/me No one is in chat. New viewers take time to appear.')
        elif style == 'number':
            self.raffle_answer = str(args[3])
            self.raffle_cost = 0
            self.raffle_entries = []
            instructions = '/me A random number between 1 and 100 has been picked. You have {} to guess it...'
            self.send(instructions.format(print_duration(sec)))
            time.sleep(sec - 10)
            self.send('/me The raffle is ending in 10 seconds...')
            time.sleep(10)
            if len(self.raffle_entries) > 0:
                winner = self.following_tag(random.choice(self.raffle_entries))
                self.send('/me ' + winner + ' won the raffle! The number was ' + self.raffle_answer + '.')
                return winner
            self.send('/me Nobody guessed the number correctly; it was ' + self.raffle_answer + '.')
        elif style == 'question':
            self.raffle_answer = str(args[4])
            self.raffle_cost = 0
            self.raffle_entries = []
            self.send('/me You have ' + print_duration(sec) + ' to answer the question: "' + args[3] + '"...')
            time.sleep(sec - 10)
            self.send('/me The raffle is ending in 10 seconds...')
            time.sleep(10)
            if len(self.raffle_entries) > 0:
                winner = self.following_tag(random.choice(self.raffle_entries))
                self.send('/me ' + winner + ' won the raffle! The answer was "' + self.raffle_answer + '".')
                return winner
            self.send('/me Nobody answered correctly; it was "' + self.raffle_answer + '".')
        elif style == 'keyword':
            self.raffle_answer = str(args[3])
            self.raffle_cost = args[1]
            self.raffle_max_entries = args[2]
            self.raffle_entries = []
            name = self.user['points']['name_singular'] if self.raffle_cost == 1 else self.user['points']['name_plural']
            plural = 's' if self.raffle_max_entries != 1 else ''
            details = 'This raffle costs {:,} {}, and you can buy up to {:,} ticket{}. '
            details = details.format(self.raffle_cost, name, self.raffle_max_entries, plural)
            info = '' if self.raffle_cost == 0 else details
            self.send('/me ' + info + 'You have ' + print_duration(sec) + ' to type "' + self.raffle_answer + '"...')
            time.sleep(sec - 10)
            self.send('/me The raffle is ending in 10 seconds...')
            time.sleep(10)
            if len(self.raffle_entries) > 0:
                winner = self.following_tag(random.choice(self.raffle_entries))
                self.send('/me ' + winner + ' won the raffle!')
                return winner
            self.send('/me Nobody entered the raffle.')
        return None

    def send(self, line):
        print('[' + self.name + '] ' + self.bot.name + ': ' + line.encode('ascii', errors='ignore').decode())
        self.bot.send('PRIVMSG #' + self.name.lower() + ' :' + line)


class RemoteControl(threading.Thread):
    def __init__(self):
        super().__init__()
        self.alive = True
        self.sock = None

    def run(self):
        print('Starting remote control')
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.bind(('0.0.0.0', 31525 if prod else 31526))
        except socket.error:
            print('Failed to bind remote control')
            time.sleep(10)
            self.run()
            return
        self.sock.listen(1)
        while self.alive:
            conn, remote = self.sock.accept()
            thread = RemoteControlConnection(conn, remote)
            thread.daemon = True
            thread.start()

    def close(self):
        self.alive = False
        self.sock.close()


class RemoteControlConnection(threading.Thread):
    def __init__(self, sock, remote):
        super().__init__()
        self.alive = True
        self.sock = sock
        self.remote = remote

    def send(self, body):
        self.sock.send((json.dumps(body) + '\n\00').encode())

    def run(self):
        remote_control.append(self)
        while self.alive:
            body = self.sock.recv(4096)
            if not body:
                break
            try:
                if body == b'\xff\xf4\xff\xfd\x06':
                    break
                body = body.decode().strip().split('\n')
                for line in body:
                    try:
                        line = json.loads(line)
                        auth = None
                        channel = None
                        channel_name = None
                        if 'token' in line.keys():
                            auth_info = api(requests.get, '', headers={'Authorization': 'Bearer ' + str(line['token'])})
                            if auth_info.status_code == 200:
                                auth = auth_info.json()['auth']
                                if auth and auth['valid'] and auth['user']:
                                    if 'edit_user' not in auth['scope']:
                                        error = 'Remote control requires scope \'edit_user\''
                                        self.send({'event': 'error', 'error': error})
                                        return
                                    if not auth['user']['twitch_login']:
                                        auth['user']['twitch_login'] = twitch_login
                                    for bot in bots.values():
                                        if auth['user']['admin'] and 'user' in line.keys():
                                            channel_name = line['user'].lower()
                                            if line['user'].lower() in bot.channels.keys():
                                                channel = bot.channels[line['user'].lower()]
                                        else:
                                            channel_name = auth['user']['sites']['twitch']['name'].lower()
                                            if channel_name in bot.channels.keys():
                                                channel = bot.channels[channel_name]
                        if type(line) != dict or 'do' not in line.keys():
                            self.send({'event': 'error', 'error': 'Missing do'})
                        elif line['do'] == 'join_channel':
                            if not auth or not auth['valid']:
                                self.send({'event': 'error', 'error': 'Invalid access token'})
                            elif channel:
                                self.send({'event': 'error', 'error': 'Nerdbot\'s already in the channel'})
                            else:
                                join_channel(auth['user'])
                                self.send({'event': 'join_channel'})
                        elif line['do'] == 'leave_channel':
                            if not auth or not auth['valid']:
                                self.send({'event': 'error', 'error': 'Invalid access token'})
                            elif not channel:
                                self.send({'event': 'error', 'error': 'Nerdbot\'s not in the channel'})
                            else:
                                for bot in bots.values():
                                    if channel_name.lower() in bot.channels.keys():
                                        bot.leave_channel(channel_name)
                                self.send({'event': 'leave_channel'})
                        elif line['do'] == 'rejoin_channel':
                            if not auth or not auth['valid']:
                                self.send({'event': 'error', 'error': 'Invalid token'})
                            else:
                                for bot in bots.values():
                                    if channel_name.lower() in bot.channels.keys():
                                        bot.leave_channel(channel_name)
                                if auth['user']['mode'] == ('' if prod else 'beta/') + 'on':
                                    join_channel(auth['user'])
                                self.send({'event': 'rejoin_channel'})
                        elif line['do'] == 'reload_channel':
                            if not auth or not auth['valid']:
                                self.send({'event': 'error', 'error': 'Invalid token'})
                            elif 'part' not in line.keys():
                                self.send({'event': 'error', 'error': 'Missing part to update'})
                            else:
                                channel.update(line['part'])
                                self.send({'event': 'reload_channel'})
                        elif line['do'] == 'get_stats':
                            channels = 0
                            for bot in bots.values():
                                channels += len(bot.channels)
                            self.send({'event': 'stats', 'bot_count': len(bots), 'channel_count': channels})
                        elif line['do'] == 'get_channel_stats':
                            self.send({'event': 'channel_stats', 'joined': channel is not None})
                        elif line['do'] == 'start_raffle':
                            if not auth or not auth['valid']:
                                self.send({'event': 'error', 'error': 'Invalid token'})
                            elif not channel:
                                self.send({'event': 'error', 'error': 'Nerdbot\'s not in the channel'})
                            else:
                                winner = None
                                if line['type'] == 'random':
                                    winner = channel.start_raffle('random', [3, 0, 1])
                                elif line['type'] == 'number' and 'number' in line.keys():
                                    winner = channel.start_raffle('number', [60, 0, 1, line['number']])
                                elif line['type'] == 'question' and 'question' in line.keys() and 'answer' in line.keys():
                                    winner = channel.start_raffle('question', [60, 0, 1, line['question'], line['answer']])
                                elif line['type'] == 'keyword' and 'keyword' in line.keys() and 'cost' in line.keys() and 'max' in line.keys():
                                    winner = channel.start_raffle('keyword', [60, int(line['cost']), int(line['max']), line['keyword']])
                                if winner:
                                    self.send({'event': 'raffle_end', 'winner': winner})
                                else:
                                    self.send({'event': 'error', 'error': 'Missing raffle options'})
                        else:
                            self.send({'event': 'error', 'error': 'Invalid do'})
                    except ValueError:
                        self.send({'event': 'error', 'error': 'Invalid JSON'})
            except:
                logging.exception('Failed to run command ' + command + ' in channel ' + self.name + ':')
        self.close()

    def close(self):
        self.alive = False
        self.sock.close()
        remote_control.remove(self)


class Sender:
    def __init__(self, twitch_id, name, level, is_sub, is_admin):
        self.twitch_id = twitch_id
        self.name = name
        self.level = level
        self.sub = is_sub
        self.admin = is_admin


def channel_thread():
    while True:
        bots_temp = bots.copy()
        for bot in bots_temp.values():
            channels_temp = bot.channels.copy()
            for channel in channels_temp.values():
                print('Running: ' + channel.name)
                try:
                    channel.main()
                except:
                    logging.exception('Failed to run ' + channel.name + '\'s main() thread:')
                time.sleep(10)
        time.sleep(30)


if __name__ == '__main__':
    try:
        logging.basicConfig(level=logging.WARN, filename='twitch.log')
        logging.warn('Bot loop started')
        link = r'([^ ]*(\.|d0t|dot)+[^a-zA-Z\d]*(com|net|me|ac|kz|se|de|ru|link|gd|gu|su|ma|ga|mp|af|es|cc|ml|fr|ly|tk|org|io|info|tv|gl|co|uk|eu|cc|nr|ua|us|biz|mobi)(\s|[^a-zA-Z\d]|\n|$)+)'
        prefixes = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '-', '=']
        oauth = {'Authorization': 'Bearer UQqmoQbkoTqQRensiMDenSRtOFliJW'}
        prod = socket.gethostbyname('api.nerdbot.tv') == '127.0.0.1'
        twitch_login = {'name': 'nerdbottv', 'token': '61ugj2elmixa4ksjn97xqeplutwmpk'}
        bots = {}
        admins = []
        emotes = []
        remote_control = []
        version = '5.1'
        atexit.register(clean_up)
        remote_control_thread = RemoteControl()
        remote_control_thread.daemon = True
        remote_control_thread.start()
        users = api(requests.get, 'users', oauth, {'mode': ('' if prod else 'beta/') + 'on'})
        user_roles = ['viewers', 'subscribers', 'regulars', 'moderators']
        start_time = time.time()
        line_queue = []
        admins_request = api(requests.get, 'admins')
        if admins_request.status_code == 200 and is_json(admins_request.text):
            for admin in admins_request.json():
                admins.append(admin['sites']['twitch']['id'])
        emotes_request = requests.get('https://cdn.rawgit.com/NerdbotTV/Resources/master/emotes/twitch.json')
        if emotes_request.status_code == 200 and is_json(emotes_request.text):
            emotes = emotes_request.json()
        new_thread = threading.Thread(target=channel_thread)
        new_thread.daemon = True
        new_thread.start()
        if users.status_code == 200:
            for user in users.json():
                join_channel(user)
                time.sleep(0.1)
        while True:
            try:
                if len(line_queue) > 0:
                    line = line_queue[0]
                    bots[line[0]].channels[line[1]].on_line(line[2])
                    del line_queue[0]
                time.sleep(0.1)
            except KeyboardInterrupt:
                break
            except:
                logging.exception('Failed to handle line:')
                del line_queue[0]
        logging.warn('Bot loop ended')
    except:
        logging.exception('Bot loop crashed:')
