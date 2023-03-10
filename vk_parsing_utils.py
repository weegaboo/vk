import requests
import warnings
import pickle
import time

from tqdm import tqdm
from functools import wraps
from datetime import datetime
from typing import Dict, List, Union, Callable, Any, Optional


class VKError(Exception):
    def __init__(self, error_code: int, error_msg: str):
        self.error_code = error_code
        self.error_msg = error_msg

    def __str__(self):
        return f"Error {self.error_code} {self.error_msg}"


class NotIncreaseError(Exception):
    def __str__(self):
        return "Current items are empty (no data or no increase)!"


class Base(object):
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
        request = requests.get("https://oauth.vk.com/token/", params=params)
        return request.json()

    def _get_access_token(self) -> Union[str, Dict]:
        """
        Получаем access_token

        """
        request = self._get_init()
        return request['access_token'] if 'access_token' in request.keys() else request

    def api_request(self, method: str, params: Dict) -> Dict:
        request = requests.get(f"https://api.vk.com/method/{method}", params=params).json()
        time.sleep(self.time)
        if 'error' in request:
            error = request['error']
            raise VKError(error['error_code'], error['error_msg'])
        response = request['response']
        if 'items' in response:
            if not len(response['items']):
                raise NotIncreaseError
        return response

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


class Wall(Base):
    """
    Methods for parsing a groups or users wall

    """

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @staticmethod
    def _cut_posts_by_date(posts: List[Dict], start_date: datetime) -> Dict:
        for post in posts:
            try:
                date = datetime.utcfromtimestamp(post['date'])
                if start_date <= date:
                    yield post
            except KeyError:
                warnings.warn(f"KeyError, post: {post}")

    @Base.add_base_params(count=1)
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
        request = self.api_request("wall.get", params)
        return request['count']

    @Base.add_base_params(count=100, offset=0, fields=', '.join(Base.base_user_fields), extended=0)
    def get_posts(self, start_date: datetime, count2load: int = None, **params) -> Dict[str, Any]:
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
            curr_data = self.api_request("wall.get", params)
            data['total_count'] = curr_data['count']
            data['items'].extend(curr_data['items'])
            if params['extended']:
                data['profiles'].extend(curr_data['profiles'])
                data['groups'].extend(curr_data['groups'])
            params['offset'] += params['count']
            pbar.update(len(data['items']) - data['loaded_count'])
            data['loaded_count'] = len(data['items'])
            last_post_date = datetime.utcfromtimestamp(data['items'][-1]['date'])
            if last_post_date < start_date:
                data['items'] = list(self._cut_posts_by_date(data['items'], start_date))
                pbar.update(steps)
                break
        pbar.close()
        return data

    @Base.add_base_params(count=1)
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

        Wall post ID.
        post_id: int (positive)

        Returns
        -------
        Amount.
        amount : int
        """
        request = self.api_request("wall.getComments", params)
        return request['count']

    @Base.add_base_params(fields=', '.join(Base.base_user_fields), extended=1)
    def get_comment(self, **params) -> Dict[str, Any]:
        return self.api_request("wall.getComment", params)

    @Base.add_base_params(count=100, offset=0, fields=', '.join(Base.base_user_fields), extended=1)
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
                curr_data = self.api_request("wall.getComments", params)
            except VKError:
                return data
            data['items'].extend(curr_data['items'])
            if params['extended']:
                data['profiles'].extend(curr_data['profiles'])
                data['groups'].extend(curr_data['groups'])
            params['offset'] += params['count']
            data['loaded_count'] = len(data['items'])
        return data


class Likes(Base):

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @Base.add_base_params(count=1000, offset=0, extended=1)
    def get_likes(self, count2load: int = 0, **params) -> Dict[str, Any]:
        """
        The method gets a list of IDs of users who have marked the specified object with a "Like" mark.

        Parameters
        ----------
        Likes count2load. By default, 0.
        count2load: int

        The type of the object. Possible types:
        • post — an entry on the user's or community's wall.
        • post_ads — advertising record.
        • comment — a comment on the entry on the wall.
        • photo — photo.
        • video — video recording.
        • note — a note.
        • market — product.
        • photo_comment — comment on the photo.
        • video_comment — comment on the video.
        • topic_comment — comment in the discussion.
        • market_comment — product comment.
        • sitepage — the page of the site where the "Like" widget is installed.
        type: str

        ID of the object owner:
        • User ID, if the object belongs to the user.
        • Community ID (with a minus sign) if the object belongs to the community.
        • Application ID, if the type parameter has the value site page.
        owner_id: int

        The ID of the object.
        item_id: int

        Information about whether to return extended information. Possible values:
        • 1 — return extended information about users and communities from the list
         of those who have marked "Like" or shared an entry.
        • 0 — return only user and community IDs.
        By default, 0.
        extended: int (checkbox)


        Returns
        -------
        data: Dict[str, Any]

        """

        data = {
            'count2load': count2load,
            'loaded_count': -1,
            'items': []
        }
        while data['loaded_count'] < data['count2load']:
            curr_data = self.api_request("likes.getList", params)
            data['items'].extend(curr_data['items'])
            data['loaded_count'] += len(curr_data['items'])
            params['offset'] += params['count']
        return data


class User(Wall, Likes):
    """
    Парсинг данных пользователей ВК

    """

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @Base.add_base_params()
    def find_user(self, **params) -> Dict:
        """
        Найти пользователя

        """
        return self.api_request("users.search", params)

    @Base.add_base_params(fields=', '.join(Base.base_user_fields))
    def get_page_data(self, **params) -> Dict:
        """
        Получить информацию о пользователе

        """
        return self.api_request("users.get", params)

    @Base.add_base_params(count=1000, offset=0, fields=', '.join(Base.base_user_fields))
    def get_followers(self, **params) -> Dict[str, Any]:
        """


        Parameters
        ----------

        Returns
        -------

        """
        page_data = self.get_page_data(user_ids=params['user_id'])
        data = {
            'total_count': page_data[0]['followers_count'],
            'loaded_count': 0,
            'members': []
        }
        pbar = tqdm(total=data['total_count'])
        while data['loaded_count'] < data['total_count']:
            try:
                curr_data = self.api_request("users.getFollowers", params)
            except NotIncreaseError:
                pbar.close()
                return data
            data['loaded_count'] = len(data['members'])
            data['members'].extend(curr_data['items'])
            params['offset'] += params['count']
            increase = len(data['members']) - data['loaded_count']
            pbar.update(increase)
        pbar.close()
        return data


class Group(Wall, Likes):
    """Vk groups parsing"""

    def __init__(self, username: str, password: str):
        super().__init__(username, password)

    @Base.add_base_params(fields='members_count')
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
        return self.api_request("groups.getById", params)

    @Base.add_base_params(fields='members_count')
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
        request = self.api_request("groups.getById", params)
        return request[0]['members_count']

    @Base.add_base_params(count=1000, offset=0, fields=', '.join(Base.base_user_fields))
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
            try:
                curr_data = self.api_request("groups.getMembers", params)
            except NotIncreaseError:
                pbar.close()
                return data
            data['members'].extend(curr_data['items'])
            data['loaded_count'] = len(data['members'])
            params['offset'] += params['count']
            increase = len(data['members']) - data['loaded_count']
            pbar.update(increase)
        pbar.close()
        return data


class VK(User, Group):

    def __init__(self, username: str, password: str):
        super().__init__(username, password)
