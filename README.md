# vk
VK parsing

How to use:

1) Init vk = VK(username, password)

2) Check vk.access_token. if you need a captcha, pass additional parameters during reinitialization \
   vk = VK(username, password, captcha_key, captcha_sid)

3) Use VK class methods with additional params
