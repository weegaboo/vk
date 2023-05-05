# vk
VK parsing

How to use:

1) from vk_parsing_utils import VK

2) vk = VK(username, password)

3) Check vk.access_token. if you need a captcha, pass additional parameters during reinitialization (https://dev.vk.com/api/captcha-error) \
   vk = VK(username, password, captcha_key, captcha_sid)

4) Use VK class methods with additional params. Check https://dev.vk.com/method
