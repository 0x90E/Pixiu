# -*- coding: utf-8 -*-
import random
from scrapy.exceptions import IgnoreRequest

class RandomUserAgentMiddleware(object):
    user_agent = []

    def __init__(self, crawler):
        super(RandomUserAgentMiddleware, self).__init__()
        user_agent_list_file = "files/user_agents"
        with open(user_agent_list_file, 'r') as f:
            self.user_agent_list = [line.strip() for line in f.readlines()]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agent_list)
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)
