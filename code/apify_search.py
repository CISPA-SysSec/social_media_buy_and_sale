import argparse
import os
import shutil
import random

import requests
from apify_client import ApifyClient
from constants import COLLECTIONS, IMAGES
from db_util import MongoDBActor

import shared_util


class APIFY_Search:
    def __init__(self, function):
        self.function = function
        self.client = ApifyClient(os.environ['APIFY_API_KEY'])

    def process(self):
        # collects both insta profile and posts meta data
        if APIFY_Search.collect_instagram_user_profile_and_posts_data_combined.__name__ == self.function:
            self.collect_instagram_user_profile_and_posts_data_combined()
        # collects facebook profile metadata
        elif APIFY_Search.collect_facebook_user_profile_data.__name__ == self.function:
            self.collect_facebook_user_profile_data()
        elif APIFY_Search.collect_youtube_user_profile_data.__name__ == self.function:
            self.collect_youtube_user_profile_data()
        elif APIFY_Search.collect_tiktok_user_profile_data.__name__ == self.function:
            self.collect_tiktok_user_profile_data()
        elif APIFY_Search.collect_facebook_user_posts_data.__name__ == self.function:
            self.collect_facebook_user_posts_data()
        elif APIFY_Search.youtube_image_download.__name__ == self.function:
            self.youtube_image_download()
        elif APIFY_Search.download_twitter_images.__name__ == self.function:
            self.download_twitter_images()

    def youtube_image_download(self):
        _download_tuples = []
        for item in MongoDBActor(COLLECTIONS.YOU_TUBE_ACCOUNTS).find():
            _data_ = item['data']
            for each_channel in _data_:
                _id = None
                _thumbnail = None
                if 'id' in each_channel:
                    _id = each_channel['id']
                if 'thumbnailUrl' in each_channel:
                    _thumbnail = each_channel['thumbnailUrl']
                    if '.jpg?' in _thumbnail:
                        _thumbnail = _thumbnail.split(".jpg?")[0]
                        _thumbnail = "{}.jpg".format(_thumbnail)
                if _id and _thumbnail:
                    _tuple = (_id, _thumbnail)
                    _download_tuples.append(_tuple)
        _len = len(_download_tuples)
        random.shuffle(_download_tuples)
        for cnt, each_tuple in enumerate(_download_tuples):
            print('Processing {}/{}, {}, {}'.format(cnt, _len, each_tuple[0], each_tuple[1]))
            _f_path = "/data/buy_and_sale_social_media_images/youtube/{}.png".format(each_tuple[0])
            if os.path.isfile(_f_path):
                print("Already downloaded {}, escaping ...".format(_f_path))
                continue
            self.download_image(each_tuple[1], _f_path)

    def download_twitter_images(self):
        download_tuples = []
        for item in MongoDBActor("twitter_user").find():
            if 'screen_name' not in item:
                continue
            _screen_name = item['screen_name']
            if 'detail' not in item:
                continue
            _detail = item['detail']
            if 'errors' in _detail:
                continue
            print(_detail)
            _data_ = _detail['data'][0]
            _profile_image_url = _data_['profile_image_url']
            _tuple = (_screen_name, _profile_image_url)
            download_tuples.append(_tuple)
        _len = len(download_tuples)
        random.shuffle(download_tuples)
        for cnt, each_tuple in enumerate(download_tuples):
            print('Processing {}/{}, {}, {}'.format(cnt, _len, each_tuple[0], each_tuple[1]))
            _f_path = "/home/USER/buy_and_sale_social_media_images/twitter/{}.png".format(each_tuple[0])
            if os.path.isfile(_f_path):
                print("Already downloaded {}, escaping ...".format(_f_path))
                continue
            self.download_image(each_tuple[1], _f_path)

    def download_image(self, request_url, save_path):
        try:
            response = requests.get(request_url, stream=True)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

            if response.status_code == 200:
                # is_exist = os.path.exists(save_path)
                # if is_exist:
                #     os.makedirs(save_path)
                with open('{}'.format(save_path), 'wb') as f:
                    response.raw.decode_content = True
                    shutil.copyfileobj(response.raw, f)
        except Exception as ex:
            print("Exception occurred in twitter request to image {}".format(ex))

    def collect_instagram_user_profile_and_posts_data_combined(self):
        _accounts = set()
        with open("report/visible_ids/instagram.txt", "r") as f_read:
            lines = f_read.readlines()
        for line in lines:
            line = line.replace("\n", "")
            _accounts.add(line)
        for cnt, account in enumerate(_accounts):
            print("Processing insta account {}/{}, {}".format(cnt, len(_accounts), account))
            self.scrape_instagram_single_profile_data(account)

    def scrape_instagram_single_profile_data(self, each_owner):
        is_found = len(set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNTS).distinct(key="id",
                                                                                 filter={
                                                                                     "id": each_owner}))) > 0
        if is_found:
            print("Continuing data already processed !... {}".format(each_owner))
            return

        run_input = {
            "usernames": [each_owner]
        }
        try:
            # Run the Actor and wait for it to finish
            run = self.client.actor("apify/instagram-profile-scraper").call(run_input=run_input)
            # Fetch and print Actor results from the run's dataset (if there are any)
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                item['id'] = each_owner
                MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNTS).insert_data(item)
                if 'profilePicUrl' in item:
                    _request_url = item['profilePicUrl']
                    _save_path = "{}/{}.png".format(IMAGES.INSTAGRAM, item['username'])
                    shared_util.download_image(_request_url, _save_path)
                    print("Image download requested .. {}".format(_save_path))
                print("Data inserted:{}".format(item))
        except Exception as ex:
            print("Exception :{}".format(ex))

    def collect_facebook_user_posts_data(self):
        _accounts = set()
        with open("report/visible_ids/facebook.txt", "r") as f_read:
            lines = f_read.readlines()
        _profiles = set()
        for l in lines:
            l = l.replace("\n", "")
            _profiles.add(l)

        for count, each_profile in enumerate(_profiles):
            print('Processing {}, {}/{}'.format(each_profile, count, len(_profiles)))
            is_found = len(set(MongoDBActor(COLLECTIONS.FACEBOOK_POSTS).distinct(key="url",
                                                                                 filter={
                                                                                     "url": each_profile}))) > 0
            if is_found:
                print("Continuing data already processed !... {}".format(each_profile))
                continue

            run_input = {
                "resultsLimit": 20,
                "startUrls": [
                    {
                        "url": each_profile
                    }
                ]
            }

            try:
                run = self.client.actor("apify/facebook-posts-scraper").call(run_input=run_input)
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    item['url'] = each_profile
                    print(item)
                    MongoDBActor(COLLECTIONS.FACEBOOK_POSTS).insert_data(item)

                    if 'user' in item:
                        _user = item['user']
                        if 'profilePic' in _user:
                            _profile_pic = _user['profilePic']
                            _user_id = _user['id']
                            _save_path = "{}/{}.png".format(IMAGES.FACEBOOK, _user_id)
                            if os.path.exists(_save_path):
                                continue
                            shared_util.download_image(_profile_pic, _save_path)
                            print("Image download requested .. {}".format(_save_path))

            except Exception as ex:
                print("Exception :{}".format(ex))

    def collect_facebook_user_profile_data(self):
        _accounts = set()
        with open("report/visible_ids/facebook.txt", "r") as f_read:
            lines = f_read.readlines()
        _profiles = set()
        for l in lines:
            l = l.replace("\n", "")
            _profiles.add(l)

        for count, each_profile in enumerate(_profiles):
            print('Processing {}, {}/{}'.format(each_profile, count, len(_profiles)))
            is_found = len(set(MongoDBActor(COLLECTIONS.FACEBOOK_ACCOUNTS).distinct(key="url",
                                                                                    filter={
                                                                                        "url": each_profile}))) > 0
            if is_found:
                print("Continuing data already processed !... {}".format(each_profile))
                continue

            run_input = {
                "language": "en-US",
                "pages": [
                    each_profile
                ]
            }

            try:
                run = self.client.actor("apify/facebook-page-contact-information").call(run_input=run_input)
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    item['url'] = each_profile
                    print(item)
                    MongoDBActor(COLLECTIONS.FACEBOOK_ACCOUNTS).insert_data(item)
            except Exception as ex:
                print("Exception :{}".format(ex))

    def collect_youtube_user_profile_data(self):
        with open("report/visible_ids/youtube.txt", "r") as f_read:
            lines = f_read.readlines()

        _accounts = set()
        for l in lines:
            l = l.replace("\n", "")
            _accounts.add(l)

        for cnt, each_you_tube in enumerate(_accounts):
            print('Processing {}, {}/{}'.format(cnt, len(_accounts), len(each_you_tube)))
            is_found = len(set(MongoDBActor(COLLECTIONS.YOU_TUBE_ACCOUNTS).distinct(key="url",
                                                                                    filter={
                                                                                        "url": each_you_tube}))) > 0
            if is_found:
                print("Continuing data already processed !... {}".format(each_you_tube))
                continue
            run_input = {
                "startUrls": [{"url": each_you_tube}],
                "maxResults": 100,
                "maxResultsShorts": None,
                "maxResultStreams": None,
            }

            try:
                # Run the Actor and wait for it to finish
                run = self.client.actor("67Q6fmd8iedTVcCwY").call(run_input=run_input)

                _found_data = []
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    _found_data.append(item)

                _all_data = {'url': each_you_tube, 'data': _found_data}
                print(_all_data)
                MongoDBActor(COLLECTIONS.YOU_TUBE_ACCOUNTS).insert_data(_all_data)
            except Exception as ex:
                print("Exception :{}".format(ex))

    def collect_tiktok_user_profile_data(self):
        with open("report/visible_ids/tiktok.txt", "r") as f_read:
            lines = f_read.readlines()

        _accounts = set()
        for l in lines:
            l = l.replace("\n", "")
            _accounts.add(l)

        for cnt, each_tik_tok_profile in enumerate(_accounts):
            print('Processing {}, {}/{}'.format(cnt, len(_accounts), len(each_tik_tok_profile)))
            is_found = len(set(MongoDBActor(COLLECTIONS.TIK_TOK_ACCOUNTS).distinct(key="id",
                                                                                   filter={
                                                                                       "id": each_tik_tok_profile}))) > 0
            if is_found:
                print("Continuing data already processed !... {}".format(each_tik_tok_profile))
                continue

            try:
                run_input = {
                    "profiles": [
                        each_tik_tok_profile
                    ],
                    "resultsPerPage": 100,
                    "shouldDownloadCovers": False,
                    "shouldDownloadSlideshowImages": False,
                    "shouldDownloadSubtitles": False,
                    "shouldDownloadVideos": False,
                    "searchSection": "",
                    "maxProfilesPerQuery": 10
                }

                # Run the Actor and wait for it to finish
                run = self.client.actor("clockworks/free-tiktok-scraper").call(run_input=run_input)

                _found_data = []
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    if "authorMeta" in item:
                        author_meta = item['authorMeta']
                        if 'avatar' in author_meta:
                            avatar_img_url = author_meta['avatar']
                            video_url = item['webVideoUrl']
                            img_name = video_url.split("/")[-1]
                            img_path = "{}/{}.png".format(IMAGES.TIK_TOK, img_name)
                            item['img_path'] = img_path
                            shared_util.download_image(avatar_img_url, img_path)
                    _found_data.append(item)

                _all_data = {'id': each_tik_tok_profile, 'data': _found_data}
                print(_all_data)
                MongoDBActor(COLLECTIONS.TIK_TOK_ACCOUNTS).insert_data(_all_data)

            except Exception as ex:
                print("Exception occurred {}".format(ex))


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="APIFY API Search")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    meta_data = APIFY_Search(_arg_value.process_name)
    meta_data.process()
