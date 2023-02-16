import requests
import pickle
import time

from typing import Any, Dict, List, Union, NoReturn, Callable
from functools import wraps


class Vk(object):
    client_id = "2274003"
    client_secret = "hHbZxrka2uZ6jB1inYsH"

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.access_token = self._get_access_token()
        self.base_params = {'v': 5.131, 'access_token': self.access_token}

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
    def add_base_params(func) -> Callable:
        @wraps(func)
        def _wrapper(self, **kwargs):
            kwargs.update(self.base_params)
            data = func(self, **kwargs)
            return data
        return _wrapper

    @staticmethod
    def save(obj, name: str):
        with open(f'{name}.pickle', 'wb') as f:
            pickle.dump(obj, f)


class ParseUser(Vk):
    """
    Парсинг данных пользователей ВК

    """
    base_fields = [
        'id', 'first_name', 'last_name',
        'is_closed', 'about', 'activities',
        'bdate', 'city', 'contacts',
        'country', 'domain', 'has_photo',
        'home_town', 'interests', 'personal',
        'quotes', 'relation', 'sex', 'status'
    ]

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    def find_user(self, **kwargs) -> Dict:
        kwargs |= self.base_params
        r = requests.get("https://api.vk.com/method/users.search/", params=kwargs)
        return r.json()

    def get_user_page_data(self, user_ids: str, fields: str = base_fields):
        """
        Получить информацию о пользователе

        """
        variables = locals()
        params = self._update_params(variables)
        r = requests.get("https://api.vk.com/method/users.get/", params=params)
        try:
            request_data = r.json()
            data = request_data['response']
        except Exception as e:
            print(f'{e}: {r}')
            return None
        return data

    def get_user_posts(self, owner_id: int, count=100) -> Dict:
        variables = locals()
        params = self._update_params(variables)
        posts = requests.get("https://api.vk.com/method/wall.get", params=params)
        return posts.json()


class ParseGroup(Vk):
    """Vk groups parsing"""

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @Vk.add_base_params
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
        data = requests.get("https://api.vk.com/method/groups.getById", params=params)
        return data.json()

    @Vk.add_base_params
    def get_posts(self, **params) -> List:
        """
        Parse posts with wall.get method
        https://dev.vk.com/method/wall.get


        Parameters
        ----------
        ID of the user or community from whose wall you want to get posts.
        ID should be with comma separation.
        owner_id: int

        Short name of the user or group
        domain: str

        The offset required to select a specific subset of records.
        offset: int (positive)

        The number of records to be retrieved. Maximum value: 100.
        count: int (positive)

        1 — additional profiles and groups fields containing information about users and communities
        will be returned in the response. By default: 0.
        extended: int (checkbox 1 or 0)

        List of additional fields to get.
        Fields for users -- https://dev.vk.com/reference/objects/user
        Fields for groups -- https://dev.vk.com/reference/objects/group
        fields: str

        Returns
        -------
        Returns a list of posts from the user's or community's wall.
        data : Dict[List]

        """
        data = {'count ': 0, 'items': []}
        if params['extended']:
            data.update({'profiles': [], 'groups': []})
        while len(data['posts']) < params['count']:
            curr_posts_data = requests.get("https://api.vk.com/method/wall.get", params=params).json()
            try:
                curr_posts = curr_posts_data['response']['items']
                posts.extend(curr_posts)
            except KeyError:
                print(curr_posts_data)
            params['offset'] += 100
            time.sleep(0.33)
        return posts

    @Vk.add_base_params
    def get_post_comments(self, **params) -> List:
        """
        Parse comments from post with wall.getComments method
        https://dev.vk.com/method/groups.getById


        Parameters
        ----------
        Owner id (user or community id). The community ID in the owner_id parameter must be specified with the "-" sign
        for example, owner_id=-1 corresponds to the VKontakte API community ID (https://vk.com/apiclub)
        owner_id: int

        The id of the post on the wall.
        post_id: int

        Vk group id or domain
        group_id: str

        List of additional fields to get (https://dev.vk.com/reference/objects/group)
        fields: str

        Returns
        -------
        List with post comments
        comments : List

        """
        comments = []
        prev_len = -1
        while prev_len < len(comments):
            prev_len = len(comments)
            curr_comments_data = requests.get(
                "https://api.vk.com/method/wall.getComments",
                params=params
            ).json()
            try:
                curr_comments = curr_comments_data['response']['items']
                comments.extend(curr_comments)
                print(len(comments))
            except KeyError:
                print(curr_comments_data)
            self.base_params['offset'] += 100
            time.sleep(0.33)
        return comments


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
    # def get_group_posts(self, owner_id: int, count=100) -> dict:
    #     params = {
    #         'v': 5.131,
    #         'access_token': self.access_token,
    #         'owner_id': owner_id,
    #         'count': count,
    #         'offset': 0
    #     }
    #     posts = requests.get(
    #         "https://api.vk.com/method/wall.get",
    #         params=params
    #     ).json()
    #     return posts
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
