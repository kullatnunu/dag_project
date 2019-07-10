import requests
from mongoengine import connect
from mongodb.gene import Url


url_num = 10100


token = "token"

connect(host="mongodb://localhost:27017/mongodb")


def api_get_keyword(url):
    headers = {"Authorization": "Bearer {}".format(token)}
    r = requests.get(f'http://12.345.678.90/v1/endpoint1?url={url}', headers=headers)
    # r = requests.get(f'http://12.345.678.90/v1/endpoint1?url={url}', headers=headers)
    print(r.json())


def get_url():
    urls = Url.objects()[url_num:20000]
    for url in urls:
        api_get_keyword(url)


from locust import HttpLocust, TaskSet, task

class WebsiteTasks(TaskSet):
    # def on_start(self):
    #     self.client.post("/", {
    #         "username": "test",
    #         "password": "123456"
    #     })

    @task(2)
    def index(self):
        self.client.get('/endpoint1?url=http://media/wifi?server=test')
        # self.client.get("/endpoint1?url=https:test")

    @task(1)
    def about(self):
        self.client.get("/endpoint2?url=https:test")

class WebsiteUser(HttpLocust):
    task_set = WebsiteTasks
    host = "http://12.345.678.90/v1"
    min_wait = 1
    max_wait = 5

#
# def main():
#     get_url()
#
#
# if __name__ == "__main__":
#     main()
