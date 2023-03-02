import json
from typing import Optional, Dict

import requests
from airflow.models import Variable


#
# @author: anhlt
#
class DebugServiceNotification:
    config_dict: dict = json.loads(Variable.get('RV_DEBUG_SERVICE_NOTIFICATION'))

    def __new__(cls, *args, **kwargs):
        raise TypeError('Static classes cannot be instantiated')

    @classmethod
    def notify(cls, title: str, sub_title_dict: Dict[str, str], msg: str, custom_spaces: Optional[list] = None):
        if cls.config_dict['google'] is not None:
            cls._notify_google_chat(title, sub_title_dict, msg, custom_spaces if custom_spaces is not None else [])

    @classmethod
    def _notify_google_chat(cls, title: str, sub_title_dict: Dict[str, str], msg: str, custom_spaces: list):
        google_config_dict = cls.config_dict['google']

        url = google_config_dict['url']
        admin_spaces = google_config_dict['spaces']

        spaces = [] + admin_spaces + custom_spaces

        messages = []
        messages.append(f"*{title}*")
        for k, v in sub_title_dict.items():
            messages.append(f"[{k}]  *{v}*")
        messages.append(f"```{msg}``` ")

        response = requests.request('POST', url=url, data=json.dumps({
            'spaces': spaces,
            'message': {
                'text': "\n".join(messages)
            }
        }))

        if response.status_code == 200:
            return True
        else:
            print('DebugServiceNotification: failed to send message to google chat.')
            print(response.text)
            return False
