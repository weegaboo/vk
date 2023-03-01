import requests
import warnings
import pickle
import time

from tqdm import tqdm
from functools import wraps
from datetime import datetime
from typing import Dict, List, Union, Callable, Any, Optional


class VkError(Exception):
    def __init__(self, error_code: int, error_msg: str):
        self.error_code = error_code
        self.error_msg = error_msg

    def __str__(self):
        return f"Error {self.error_code} {self.error_msg}"


class Vk(object):
    client_id = "2274003"
    client_secret = "hHbZxrka2uZ6jB1inYsH"
    base_user_fields = [
        'id', 'first_name', 'last_name',
        'is_closed', 'about', 'activities',
        'bdate', 'city', 'contacts', 'followers_count',
        'country', 'domain', 'has_photo',
        'home_town', 'interests', 'personal',
        'quotes', 'relation', 'sex', 'status'
    ]
    time = 0.33
    version = 5.131

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.access_token = self._get_access_token()
        self.base_params = {'v': self.version, 'access_token': self.access_token}

    def _get_init(self):
        params = {
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password
        }
        r = requests.get("https://oauth.vk.com/token/", params=params)
        return r.json()

    def _get_access_token(self) -> Union[str, Dict]:
        """
        Получаем access_token

        """
        r = self._get_init()
        return r['access_token'] if 'access_token' in r.keys() else r

    @staticmethod
    def make_request(method: str, params: Dict) -> Dict:
        request = requests.get(f"https://api.vk.com/method/{method}", params=params).json()
        if 'error' in request:
            error = request['error']
            raise VkError(error['error_code'], error['error_msg'])
        return request

    @staticmethod
    def add_base_params(**params):
        def decorator(func) -> Callable:
            @wraps(func)
            def _wrapper(self, **kwargs):
                kwargs |= self.base_params
                for param, value in params.items():
                    if param not in kwargs:
                        kwargs[param] = value
                data = func(self, **kwargs)
                return data
            return _wrapper
        return decorator

    @staticmethod
    def save(obj, name: str):
        with open(f'{name}.pickle', 'wb') as f:
            pickle.dump(obj, f)

    @staticmethod
    def open_pickle(path: str):
        with open(path, 'rb') as f:
            obj = pickle.load(f)
        return obj


class ParseUser(Vk):
    """
    Парсинг данных пользователей ВК

    """

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    def find_user(self, **kwargs) -> Dict:
        kwargs |= self.base_params
        r = requests.get("https://api.vk.com/method/users.search/", params=kwargs)
        return r.json()

    @Vk.add_base_params(fields=', '.join(Vk.base_user_fields))
    def get_page_data(self, **params) -> List:
        """
        Получить информацию о пользователе

        """
        r = requests.get("https://api.vk.com/method/users.get/", params=params)
        data = []
        try:
            request_data = r.json()
            data = request_data['response']
        except Exception as e:
            warnings.warn(f'{e}: {r}')
        return data

    @Vk.add_base_params()
    def get_user_posts(self, **params) -> Dict:
        posts = requests.get("https://api.vk.com/method/wall.get", params=params)
        return posts.json()

    @Vk.add_base_params(count=1000, offset=0, fields=', '.join(Vk.base_user_fields))
    def get_followers(self, **params) -> Dict[str, Any]:
        page_data = self.get_page_data(user_ids=params['user_id'])
        data = {
            'total_count': page_data[0]['followers_count'],
            'loaded_count': 0,
            'members': []
        }
        pbar = tqdm(total=data['total_count'])
        while data['loaded_count'] < data['total_count']:
            request = requests.get("https://api.vk.com/method/users.getFollowers/", params=params).json()
            try:
                curr_data = request['response']
                data['members'].extend(curr_data['items'])
            except KeyError:
                warnings.warn(f"KeyError: {request}")
            params['offset'] += params['count']
            time.sleep(self.time)
            increase = len(data['members']) - data['loaded_count']
            if not increase:
                break
            pbar.update(increase)
            data['loaded_count'] = len(data['members'])
        pbar.close()
        return data


class ParseGroup(Vk):
    """Vk groups parsing"""

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @Vk.add_base_params(fields='members_count')
    def get_group_data(self, **params) -> Dict[str, List[Dict]]:
        """
        Parse community data with groups.getById method
        https://dev.vk.com/method/groups.getById

        Parameters
        ----------
        VK groups IDs or domains (max value 500). IDs should be with comma separation.
        group_ids: str

        VK group ID or domain
        group_id: str

        List of additional fields to get (https://dev.vk.com/reference/objects/group)
        fields: str

        Returns
        -------
        data : Dict[List]

        """
        request = self.make_request("groups.getById", params)
        return request

    @Vk.add_base_params(count=1)
    def get_posts_amount(self, **params):
        """
        Get posts amount

        Parameters
        ----------
        Owner id (user or community id). The community ID in the owner_id parameter must be specified with the "-" sign
        for example, owner_id=-1 corresponds to the VKontakte API community ID (https://vk.com/apiclub).
        owner_id: int

        Short name of the user or group. If domain is incorrect func will return your client posts.
        domain: str

        Returns
        -------
        Amount.
        amount : int
        """
        request = self.make_request("wall.get", params)
        return request['response']['count']

    @staticmethod
    def _cut_posts_by_date(posts: List[Dict], start_date: datetime) -> Dict:
        for post in posts:
            try:
                date = datetime.utcfromtimestamp(post['date'])
                if start_date <= date:
                    yield post
            except KeyError:
                warnings.warn(f"KeyError, post: {post}")

    @Vk.add_base_params(count=100, offset=0, fields=', '.join(Vk.base_user_fields), extended=0)
    def get_posts(self, start_date: datetime, count2load: int = None, **params) -> Dict[str, Union[Optional[List[Any]], Any]]:
        """
        Parse posts with wall.get method
        https://dev.vk.com/method/wall.get

        Parameters
        ----------
        Date until which posts will be collected (Include start date posts).
        start_date: datetime

        Owner id (user or community id). The community ID in the owner_id parameter must be specified with the "-" sign
        for example, owner_id=-1 corresponds to the VKontakte API community ID (https://vk.com/apiclub)
        owner_id: int

        Short name of the user or group. If domain is incorrect func will return your client posts
        domain: str

        The number of posts to load (total posts count by default)
        count2load: int (positive)

        Additional params:

        The offset required to select a specific subset of records.
        offset: int (positive)

        1 — additional profiles and groups fields containing information about users and communities
        will be returned in the response. By default: 0.
        extended: int (checkbox 1 or 0)

        Only if extended=1.  Returns list of additional fields to get.
        Fields for users -- https://dev.vk.com/reference/objects/user
        Fields for groups -- https://dev.vk.com/reference/objects/group
        fields: str ()

        Returns
        -------
        Returns a list of posts from the user's or community's wall.
        data : Dict[str, Union[Optional[List[Any]], Any]]

        """
        if 'owner_id' in params:
            id_ = {'owner_id': params['owner_id']}
        else:
            id_ = {'domain': params['domain']}
        total_count = self.get_posts_amount(**id_)
        if count2load is None:
            count2load = total_count
        data = {'total_count': total_count, 'loaded_count': 0, 'items': []}
        if params['extended']:
            data |= {'profiles': [], 'groups': []}
        steps = min(count2load, total_count)
        pbar = tqdm(total=steps)
        while data['loaded_count'] < count2load and data['loaded_count'] < total_count:
            request = self.make_request("wall.get", params)
            curr_data = request['response']
            data['total_count'] = curr_data['count']
            data['items'].extend(curr_data['items'])
            if params['extended']:
                data['profiles'].extend(curr_data['profiles'])
                data['groups'].extend(curr_data['groups'])
            params['offset'] += params['count']
            time.sleep(self.time)
            pbar.update(len(data['items']) - data['loaded_count'])
            data['loaded_count'] = len(data['items'])
            last_post_date = datetime.utcfromtimestamp(data['items'][-1]['date'])
            if last_post_date < start_date:
                data['items'] = list(self._cut_posts_by_date(data['items'], start_date))
                pbar.update(steps)
                break
        pbar.close()
        return data

    @Vk.add_base_params(count=1)
    def get_comments_amount(self, **params):
        """
        Get comments amount

        Parameters
        ----------
        Owner id (user or community id). The community ID in the owner_id parameter must be specified with the "-" sign
        for example, owner_id=-1 corresponds to the VKontakte API community ID (https://vk.com/apiclub).
        owner_id: int

        Short name of the user or group. If domain is incorrect func will return your client posts.
        domain: str

        Returns
        -------
        Amount.
        amount : int
        """
        request = self.make_request("wall.getComments", params)
        time.sleep(self.time)
        return request['response']['count']

    @Vk.add_base_params(count=100, offset=0, fields=', '.join(Vk.base_user_fields), extended=1)
    def get_comments(self, count2load: int = None, **params) -> Dict[str, Any]:
        """
        Parse comments from post with wall.getComments method
        https://dev.vk.com/method/wall.getComments


        Parameters
        ----------
        Owner id (user or community id). The community ID in the owner_id parameter must be specified with the "-" sign
        for example, owner_id=-1 corresponds to the VKontakte API community ID (https://vk.com/apiclub).
        owner_id: int

        Post ID.
        post_id: int

        Additional params:

        Number of comments required to upload. if None -> load all comments.
        count2load: int

        1 — return information about likes.
        need_likes: int (checkbox 1 or 0)

        The number of comments to be received. Default: 100.
        count: int (positive)

        The offset required to select a specific subset of records.
        offset: int (positive)

        Sort order of comments. Possible values:
        'asc' -- from old to new;
        'desc' -- from new to old.
        sort: str

        1 — additional profiles and groups fields containing information
        about users and communities will be returned. Here, by default, it is 1.
        extended: int (checkbox 1 or 0)

        Only if extended=1:
        List of additional fields to get
        1) Group: https://dev.vk.com/reference/objects/group
        2) User: https://dev.vk.com/reference/objects/user
        fields: str

        Returns
        -------
        Comments data
        data : Dict[str, Any]

        """
        if count2load is None:
            count2load = self.get_comments_amount(owner_id=params['owner_id'], post_id=params['post_id'])
        data = {'total_count': count2load, 'loaded_count': 0, 'items': []}
        if params['extended']:
            data |= {'profiles': [], 'groups': []}
        if isinstance(count2load, dict):
            warnings.warn(f'{count2load}')
        while data['loaded_count'] < count2load:
            try:
                request = self.make_request("wall.getComments", params)
            except VkError:
                return data
            curr_data = request['response']
            if not len(curr_data['items']):
                return data
            data['items'].extend(curr_data['items'])
            if params['extended']:
                data['profiles'].extend(curr_data['profiles'])
                data['groups'].extend(curr_data['groups'])
            params['offset'] += params['count']
            time.sleep(self.time)
            data['loaded_count'] = len(data['items'])
        return data

    @Vk.add_base_params(count=1000, offset=0)
    def get_likes(self, **params) -> Dict[str, Any]:
        """


        Parameters
        ----------

        Returns
        -------

        """

        pass

    @Vk.add_base_params(fields='members_count')
    def get_members_count(self, **params) -> int:
        """
        Returns members count
        https://dev.vk.com/method/groups.getById


        Parameters
        ----------
        Community identifiers or short names. The maximum number of identifiers is 500.
        group_ids: str

        Community identifier or short name.
        group_id: str

        Returns
        -------
        Comments data
        members_count : int

        """
        request = self.make_request("groups.getById", params)
        return request['response'][0]['members_count']

    @Vk.add_base_params(count=1000, offset=0, fields=', '.join(Vk.base_user_fields))
    def get_members(self, **params) -> Dict[str, Any]:
        """
        Returns a list of community members.
        https://dev.vk.com/method/groups.getMembers


        Parameters
        ----------
        Community ID or short name.
        group_id: str


        Additional params
        ----------
        The sort with which to return the list of members. Can take values:
        • 'id_asc' - in ascending order of ID;
        • 'id_desc' - in descending order of ID;
        • 'time_asc' - in chronological order by joining the community;
        • 'time_desc' - in anti-chronological order by joining the community.
        Sorting by time_asc and time_desc is only possible when called by a community moderator.
        sort: str

        The bias required to sample a specific subset of participants. The default is 0.
        offset: int (positive)

        The number of community members to get information about. Default: 1000.
        count: int (positive)

        List of additional fields to get https://dev.vk.com/reference/objects/user
        fields: str

        • 'friends' - Only friends in this community will be returned.
        • 'unsure' - Users who selected "Maybe going" will be returned (if the community is an event).
        • 'managers' - Only community leaders will be returned (available when requested by passing
          an access_token on behalf of a community administrator).
        • 'donut' - only donuts will be returned (users who have a paid VK Donut subscription).
        filter: str

        Returns
        -------
        Comments data
        data : Dict[str, Any]

        """
        data = {
            'total_count': self.get_members_count(group_id=params['group_id']),
            'loaded_count': 0,
            'members': []
        }
        pbar = tqdm(total=data['total_count'])
        while data['loaded_count'] < data['total_count']:
            request = self.make_request("groups.getMembers", params)
            curr_data = request['response']
            data['members'].extend(curr_data['items'])
            params['offset'] += params['count']
            time.sleep(self.time)
            increase = len(data['members']) - data['loaded_count']
            if not increase:
                break
            pbar.update(increase)
            data['loaded_count'] = len(data['members'])
        pbar.close()
        return data
    #
    #
    #
    #
    # @staticmethod
    # def _get_group_members_count(params: dict) -> int:
    #     params['fields'] = 'members_count'
    #     js = requests.get("https://api.vk.com/method/groups.getById", params=params).json()
    #     try:
    #         members_count = js['response'][0]['members_count']
    #     except KeyError:
    #         return -1
    #     return members_count
    #
    # def _get_groups_members_count(self, groups: list):
    #     self.groups_members_count = {}
    #     for group in tqdm(groups):
    #         params = {'v': 5.131, 'access_token': self.access_token, 'group_id': group}
    #         self.groups_members_count[group] = self._get_group_members_count(params=params)
    #         time.sleep(0.3)
    #
    # def get_comments(self, group_domain: str):
    #     # params={
    #     #     'v': 5.131,
    #     #     'access_token': self.access_token,
    #     #     'domain': group_domain,
    #     #     'offset': 0,
    #     #     'count': 100
    #     # }
    #     # while
    #     #     r = requests.get(
    #     #         "https://api.vk.com/method/wall.get/",
    #     #         params=params
    #     #     )
    #     #
    #
    #     pass
    #
    # def get_groups_members_count(self, groups: list) -> dict:
    #     """
    #     Кол-во участников в группе
    #     На вход подается список доменов групп
    #     На выходе словарь {домен: кол-во}
    #
    #     """
    #     groups_members_count = dict()
    #     for group in tqdm(groups):
    #         params={'v': 5.131, 'access_token': self.access_token, 'group_id': group}
    #         groups_members_count[group] = self._get_group_members_count(params=params)
    #         time.sleep(0.3)
    #     return groups_members_count
    #
    # def _sort_groups_by_members_count(self, groups: list):
    #     self._get_groups_members_count(groups)
    #     groups_sorted = sorted(self.groups_members_count, key=self.groups_members_count.get)
    #     self.groups = list()
    #     for group in groups_sorted:
    #         if 0 < self.groups_members_count[group]:
    #             self.groups.append(group)
    #
    # def _extend_multiple_lists(self, lst: list) -> list:
    #     result = []
    #     for inner_lst in lst:
    #         result.extend(inner_lst)
    #     return result
    #
    # def _delete_key(self, d, key):
    #     if key in d.keys():
    #         del d[key]
    #
    # def parse_group(self, params: dict) -> list:
    #     """
    #     Генератор
    #     Парсинг данных группы с определенным набором параметров
    #
    #     """
    #     params['offset'] = 0
    #     params['count'] = 1000
    #     group_name = params['group_id']
    #     members_count = 1
    #     while params['offset'] < members_count:
    #         r = requests.get(
    #             "https://api.vk.com/method/groups.getMembers/",
    #             params=params
    #         )
    #         try:
    #             r = r.json()
    #         except Exception as e:
    #             # self.err_logs.append(('Error', r))
    #             print(f'err: {e}')
    #             params['offset'] += params['count']
    #             continue
    #         try:
    #             members_count = r['response']['count']
    #             users_data = r['response']['items']
    #             if not users_data:
    #                 return
    #             yield users_data
    #         except KeyError:
    #             self.err_logs.append(('KeyError', r))
    #             print(r)
    #             return
    #         params['offset'] += params['count']
    #         clear_output(wait=True)
    #         time.sleep(1)
    #         if members_count < params['offset']:
    #             print(f'members_count: {members_count}')
    #         print(f'процесс: {round(params["offset"] / members_count * 100, 2)}')
    #         print(f'members_count: {members_count}')
    #
    # def parse_groups(self, groups: list, fields: list, save_threshold: int = 1e-4):
    #     """
    #     Парсинг данных групп с определенным набором параметров fields
    #
    #     """
    #     self.groups = groups
    #     params = {
    #         'v': 5.131,
    #         'access_token': self.access_token,
    #         'fields': ', '.join(fields)
    #     }
    #     for group in tqdm(self.groups):
    #         if group[:4] == 'club' and group[4:].isdigit():
    #             params['group_id'] = group[4:]
    #         elif group[:6] == 'public' and group[6:].isdigit():
    #             params['group_id'] = group[6:]
    #         else:
    #             params['group_id'] = group
    #             # if members_threshold < self.groups_members_count[group]:
    #             #     continue
    #         group_members = self._extend_multiple_lists(
    #             list(
    #                 self.parse_group(params)
    #             )
    #         )
    #         self.groups_data[group] = group_members
    #         if save_threshold < len(self.groups_data[group]):
    #             self.save(self.groups_data, 'groups_data')
    #         time.sleep(1)
    #
    #
    # def parse_page_followers(self, user_id, fields):
    #     params = {
    #         'v': 5.131,
    #         'access_token': self.access_token,
    #         'fields': ', '.join(fields),
    #         'user_id': user_id
    #     }
    #     params['offset'] = 0
    #     params['count'] = 1000
    #     members_count = 1
    #     while params['offset'] < members_count:
    #         r = requests.get(
    #             "https://api.vk.com/method/users.getFollowers/",
    #             params=params
    #         )
    #         try:
    #             r = r.json()
    #         except Exception as e:
    #             # self.err_logs.append(('Error', r))
    #             print(f'err: {e}')
    #             params['offset'] += params['count']
    #             continue
    #         try:
    #             members_count = r['response']['count']
    #             users_data = r['response']['items']
    #             if not users_data:
    #                 return
    #             yield users_data
    #         except KeyError:
    #             self.err_logs.append(('KeyError', r))
    #             print(r)
    #             return
    #         params['offset'] += params['count']
    #         clear_output(wait=True)
    #         time.sleep(1)
    #         if members_count < params['offset']:
    #             print(f'members_count: {members_count}')
    #         print(f'процесс: {round(params["offset"] / members_count * 100, 2)}')
    #         print(f'members_count: {members_count}')
    #
    #
    #
    #
    # def get_user_page_data(self, user_ids: list, fields: list) -> dict:
    #     params = {
    #         'v': 5.131,
    #         'user_ids': ', '.join(user_ids),
    #         'access_token': self.access_token,
    #         'fields': ', '.join(fields)
    #     }
    #     r = requests.get(
    #         "https://api.vk.com/method/users.get/",
    #         params=params
    #     )
    #     try:
    #         request_data = r.json()
    #     except Exception as e:
    #         # self.err_logs.append(('Error', r))
    #         print(f'err: {r}')
    #         return None
    #     try:
    #         data = request_data['response']
    #     except KeyError:
    #         print(request_data)
    #         return None
    #     return data
    #
    # def get_user_groups(self, user_id: str) -> dict:
    #     params = {
    #         'v': 5.131,
    #         'user_id': user_id,
    #         'access_token': self.access_token,
    #         'extended': 1,
    #         'count': 200
    #     }
    #     r = requests.get(
    #         "https://api.vk.com/method/users.getSubscriptions/",
    #         params=params
    #     )
    #     try:
    #         r = r.json()
    #     except Exception as e:
    #         # self.err_logs.append(('Error', r))
    #         print(f'err: {e}')
    #         return None
    #     return r
    #
    # def get_users_info(self, user_ids: list, fields: list) -> dict:
    #     users_data = self.get_user_page_data(user_ids, fields)
    #     for user_data in tqdm(users_data):
    #         try:
    #             if user_data['is_closed']:
    #                 continue
    #         except Exception as e:
    #             pass
    #         groups_data = self.get_user_groups(user_data['id'])
    #         groups = list()
    #         try:
    #             items = groups_data['response']['items']
    #         except Exception as e:
    #             print(groups_data)
    #             continue
    #         for group in groups_data['response']['items']:
    #             if 'screen_name' in group.keys():
    #                 groups.append(group['screen_name'])
    #             else:
    #                 groups.append(group['id'])
    #         user_data['groups'] = groups
    #     return users_data
    #
    # def find_user(self, *args, **kwargs) -> dict:
    #     params = kwargs
    #     params['access_token'] = self.access_token
    #     params['v'] = 5.131
    #     r = requests.get(
    #         "https://api.vk.com/method/users.search/",
    #         params=params
    #     ).json()
    #     return r
