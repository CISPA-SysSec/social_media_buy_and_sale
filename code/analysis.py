import argparse
import json
import os
import statistics

import constants
from db_util import MongoDBActor


class Analysis:
    def __init__(self, function_name):
        self.function_name = function_name

    def process(self):
        if self.function_name == "create_table_marketplace_url":
            self.create_table_marketplace_url()
        elif self.function_name == "create_table_marketplace_seller":
            self.create_table_marketplace_seller()
        elif self.function_name == "create_table_marketplace_social_media":
            self.create_table_marketplace_social_media()
        elif self.function_name == "create_table_marketplace_seller_nationality":
            self.create_table_marketplace_key_result('seller_nationality')
        elif self.function_name == "create_table_marketplace_posts":
            self.create_table_marketplace_posts()
        elif self.function_name == "activeness_analysis":
            self.activeness_analysis()
        elif self.function_name == "key_value_analysis":
            self.key_value_analysis()
        elif self.function_name == "fetch_followers_metrics":
            self.fetch_followers_metrics()
        elif self.function_name == "fetch_posts_metrics":
            self.fetch_posts_metrics()
        elif self.function_name == "fetch_created_date_metrics":
            self.fetch_created_date_metrics()
        elif self.function_name == "pre_process_created_date_file":
            self.pre_process_created_date_file()

    def pre_process_created_date_file(self):
        with open("report/attributes/all_metrics/created_date.json", "r") as f_read:
            data = json.load(f_read)

        all_year_data = {}
        for key, values in data.items():
            _year_data = []
            for v in values:
                if "-" in v:
                    _splitter = v.split("-")
                    _year = _splitter[0]
                    _year = _year.strip()
                elif " " in v:
                    _splitter = v.split(" ")
                    if len(_splitter) < 3:
                        _year = 2024
                    else:
                        _year = _splitter[-1]
                        _year = _year.strip()
                else:
                    _year = v
                _year_data.append(int(_year))
                _year_data.sort(reverse=False)
            all_year_data[key] = _year_data

        with open("report/attributes/all_metrics/visible_id_processed_created_date.json", "w") as f_read:
            json.dump(all_year_data, f_read, indent=4)

    def fetch_followers_metrics(self):
        _followers = {
            'twitter': "detail.data.public_metrics.followers_count",
            'instagram': "followersCount",
            'tiktok': "data.friends",
            'facebook': "followers",
            'youtube': 'data.numberOfSubscribers'
        }
        self.get_digit_metrics(_followers, 'followers', 'descend')

    def fetch_posts_metrics(self):
        _posts = {
            'twitter': "detail.data.public_metrics.tweet_count",
            'instagram': "postsCount",
            'tiktok': "data.authorMeta.heart",
            'facebook': "likes",
            'youtube': 'data.viewCount'
        }
        self.get_digit_metrics(_posts, 'posts', 'descend')

    def fetch_created_date_metrics(self):
        _created_date = {
            'twitter': "detail.data.created_at",  # created_at: '2023-10-19T13:31:36.000Z',
            'instagram': "latestPosts.timestamp",  # '2023-09-09T23:38:04.000Z',
            'tiktok': "data.createTimeISO",  # '2020-02-07T05:04:56.000Z'
            'facebook': "creation_date",  # 'May 28, 2023'
            'youtube': 'data.channelJoinedDate'  # Apr 1, 2020
        }
        self.get_digit_metrics(_created_date, 'created_date', _sort='ascend')

    # "twitter": "detail.data.public_metrics.followers_count"
    def get_digit_metrics(self, _key_metric, f_label, _sort='ascend'):
        _twitter_metrics = []
        _instagram_metrics = []
        _you_tube_metrics = []
        _facebook_metrics = []
        _tiktok_metrics = []

        _twitter_users = set(MongoDBActor("twitter_user").distinct(key="screen_name"))
        for _tu in _twitter_users:
            _found = set(MongoDBActor("twitter_user").distinct(key=_key_metric['twitter'],
                                                               filter={"screen_name": _tu}))
            if not _found:
                continue
            _found = list(_found)
            if _sort == 'ascend':
                _found.sort(reverse=False)
            else:
                _found.sort(reverse=True)
            _twitter_metrics.append(_found[0])

        _instagram_users = set(MongoDBActor("instagram_user").distinct(key="inputUrl"))
        for _tu in _instagram_users:
            _found = set(MongoDBActor("instagram_user").distinct(key=_key_metric['instagram'],
                                                                 filter={"inputUrl": _tu}))
            if not _found:
                if f_label == 'created_date':
                    _found = ['2024']
                else:
                    continue
            _found = list(_found)
            if _sort == 'ascend':
                _found.sort(reverse=False)
            else:
                _found.sort(reverse=True)
            _instagram_metrics.append(_found[0])

        _tiktok_users = set(MongoDBActor("tiktok_user").distinct(key="id"))
        for _tu in _tiktok_users:
            _found = set(MongoDBActor("tiktok_user").distinct(key=_key_metric['tiktok'],
                                                              filter={"id": _tu}))
            if not _found:
                continue
            _found = list(_found)
            if _sort == 'ascend':
                _found.sort(reverse=False)
            else:
                _found.sort(reverse=True)
            if f_label == "posts":
                _tiktok_metrics.append(sum(_found))
            else:
                _tiktok_metrics.append(_found[0])

        _facebook_users = set(MongoDBActor("facebook_user").distinct(key="facebookUrl"))
        for _tu in _facebook_users:
            _found = set(MongoDBActor("facebook_user").distinct(key=_key_metric['facebook'],
                                                                filter={"facebookUrl": _tu}))
            if not _found:
                continue
            _found = list(_found)
            if _sort == 'ascend':
                _found.sort(reverse=False)
            else:
                _found.sort(reverse=True)
            _facebook_metrics.append(_found[0])

        _youtube_users = set(MongoDBActor("youtube_user").distinct(key="url"))
        for _tu in _youtube_users:
            _found = set(MongoDBActor("youtube_user").distinct(key=_key_metric['youtube'],
                                                               filter={"url": _tu}))
            if not _found:
                continue
            _found = list(_found)
            if _sort == 'ascend':
                _found.sort(reverse=False)
            else:
                _found.sort(reverse=True)
            _you_tube_metrics.append(max(_found))

        _all_data = {
            'Facebook': _facebook_metrics,
            'X': _twitter_metrics,
            'YouTube': _you_tube_metrics,
            'TikTok': _tiktok_metrics,
            'Instagram': _instagram_metrics
        }

        _dir_path = "report/attributes/all_metrics"
        _f_path = "{}/{}.json".format(_dir_path, f_label)

        if not os.path.exists(_dir_path):
            os.makedirs(_dir_path)

        with open(_f_path, "w") as f_write:
            json.dump(_all_data, f_write, indent=4)

    def key_value_analysis(self):
        all_social_media = {
            'twitter': {
                'key': "screen_name",
                'collections': [
                    {
                        'name': 'twitter_user',
                        'attributes': {
                            'verified': 'detail.data.verified',
                            'description': 'detail.data.description',
                            'name': 'detail.data.name',
                            'protected': 'detail.data.protected'
                        }
                    },
                    {
                        'name': 'twitter_timeline',
                        'attributes': {
                            'lang': 'lang',
                            'context_domain': 'context_annotations.domain.name',
                            'context_entity': 'context_annotations.entity.name',
                            'tweet_text': 'text',
                            'expanded_url': 'entities.urls.expanded_url',
                            'tweet_mentions': 'entities.mentions.username'
                        }
                    }
                ]
            }, 'instagram': {
                'key': "inputUrl",
                'collections': [
                    {
                        'name': 'instagram_user',
                        'attributes': {
                            'fullname': 'fullname',
                            'biography': 'biography',
                            'has_channel': 'hasChannel',
                            'isBusinessAccount': 'isBusinessAccount',
                            'businessCategoryName': 'businessCategoryName',
                            'is_private': 'private',
                            'verified': 'verified'
                        }
                    }
                ]
            }, 'tiktok': {
                'key': 'id',
                'collections': [
                    {
                        'name': 'tiktok_user',
                        'attributes': {
                            'name': 'data.name',
                            'nickname': 'data.nickName',
                            'signature': 'data.signature',
                            'verified': 'data.verified',
                            'commerce_info': 'data.commerceUserInfo.commerceUser',
                            'private': 'data.privateAccount',
                            'region': 'data.region',
                            'bio_link': 'data.bioLink'
                        }
                    }
                ]
            }, 'youtube': {
                'key': 'url',
                'collections': [
                    {
                        'name': 'youtube_user',
                        'attributes': {
                            'channelName': 'data.channelName',
                            'verified': 'data.isChannelVerified',
                            'location': 'data.channelLocation',
                            'channel_type': 'data.channelType',
                            'title': 'data.channelTitle',
                            'channel_links': 'data.channelDescriptionLinks',
                            'regular_title': 'data.title',
                            'channel_description': 'data.channelDescription',
                            'avator_url': 'data.channelAvatarUrl',
                            'thumbnail': 'data.thumbnailUrl'
                        }
                    }
                ]
            }, 'facebook': {
                'key': 'facebookUrl',
                'collections': [
                    {
                        'name': 'facebook_user',
                        'attributes': {
                            'categories': 'categories',
                            'title': 'title',
                            'is_business': 'pageAdLibrary.is_business_page_active',
                            'work': 'WORK',
                            'education': 'EDUCATION',
                            'city': 'CURRENT_CITY',
                            'relationship': 'relationship',
                            'ad_status': 'ad_status',
                            'rating': 'rating',
                            'email': 'email',
                            'phone': 'phone',
                            'services': 'services',
                            'alternative_social_media': 'alternativeSocialMedia',
                            'website': 'website',
                            'about_me_urls': 'about_me.urls',
                            'info': 'info',
                            'address': 'address',
                            'page_name': 'pageName'

                        }
                    }
                ]
            }
        }

        for social_media, entries in all_social_media.items():
            _key = entries['key']
            _collections = entries['collections']
            for _collection in _collections:
                _name = _collection['name']
                _attributes = _collection['attributes']
                _attributes_values = list(_attributes.values())
                for each_attribute in _attributes_values:
                    _total_accounts = set()
                    _distinct_found = set(MongoDBActor(_name).distinct(key=each_attribute))
                    _tuple_data = []
                    for each_found in _distinct_found:
                        _found_accounts = set(MongoDBActor(_name).distinct(key=_key,
                                                                           filter={each_attribute: each_found}))
                        _total_accounts = _total_accounts.union(_found_accounts)
                        _tuple_data.append((each_found, len(_found_accounts)))
                    _tuple_data.append(('Total', len(_total_accounts)))
                    _tuple_data.sort(key=lambda x: x[1], reverse=True)
                    rows = ["{}, Count".format(each_attribute)]
                    for _t in _tuple_data:
                        rows.append("{},{}".format(_t[0], _t[1]))

                    _dir_path = "report/attributes/{}".format(social_media)
                    _f_path = "{}/{}.csv".format(_dir_path, each_attribute)

                    if not os.path.exists(_dir_path):
                        os.makedirs(_dir_path)
                    with open(_f_path, "w") as f_write:
                        for item in rows:
                            f_write.write("{}\n".format(item))

    def activeness_analysis(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}

        _all_title = []
        _all_dates = {}
        for line in lines:
            line = dict(line)
            if '_id' in line:
                _all_title.append(line['_id']['$oid'])
            if 'date_time' in line:
                _found_date = line['date_time']
                if not _found_date:
                    continue
                for _fd in _found_date:
                    if _fd not in _all_dates:
                        _all_dates[_fd] = [line['_id']['$oid']]
                    else:
                        _all_dates[_fd] = [line['_id']['$oid']] + _all_dates[_fd]
            else:
                continue

        _tuple = []
        _all_found_data = []

        # with open("report/graph_data/followers.json", "w") as f_write:
        #     json.dump(_all_platform, f_write, indent=4)
        for k, v in _all_dates.items():
            _tuple.append((k, len(v)))
            _all_found_data.append(len(v))
            # print("social media: {}, Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(k,
            #                                                                               len(v),
            #                                                                               statistics.median(v),
            #                                                                               min(v),
            #                                                                               max(v),
            #                                                                               sum(v)
            #                                                                               ))

        _tuple.sort(key=lambda x: x[0], reverse=False)

        print(_tuple)

        # print("{}".format(_tuple[0]))
        # print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
        #     len(_tuple),
        #     statistics.median(_all_found_data),
        #     min(_all_found_data),
        #     max(_all_found_data),
        #     sum(_all_found_data)
        # ))

        _arranged_dates = []
        for p in _tuple:
            _arranged_dates.append(p[0])

        _first_seen = {}
        _last_seen = {}
        _cumulative = {}
        _prev = set()
        cnt = 1
        for _a in _arranged_dates:
            _first_seen[cnt] = len(set(_all_dates[_a]).difference(set(_prev)))
            _last_seen[cnt] = len(_prev.difference(set(_all_dates[_a])))
            _prev = _prev.union(set(_all_dates[_a]))
            _cumulative[cnt] = len(_prev)
            cnt = cnt + 1

        for k, v in _first_seen.items():
            print("firstseen", k, v)

        for k, v in _last_seen.items():
            print("lastseen", k, v)

        with open("report/graph_data/activeness.json", "w") as f_write:
            json.dump({'Cummulative': _cumulative, 'Active': _last_seen}, f_write, indent=4)

    def intersection(self, lst1, lst2):
        lst3 = [value for value in lst1 if value in lst2]
        return lst3

    def create_table_marketplace_country(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}

        for line in lines:
            line = dict(line)
            if 'followers' in line:
                _found_seller = line['followers']
                if not _found_seller:
                    continue
                if type(_found_seller) is not int and type(_found_seller) is not float:
                    continue
            else:
                continue

            if 'social_media' in line:
                _found_platform = line['social_media']
            else:
                continue
            if _found_platform not in _all_platform:
                _all_platform[_found_platform] = [_found_seller]
            else:
                _all_platform[_found_platform] = list(set([_found_seller] + _all_platform[_found_platform]))

        _tuple = []
        _all_found_data = []

        with open("report/graph_data/followers.json", "w") as f_write:
            json.dump(_all_platform, f_write, indent=4)
        for k, v in _all_platform.items():
            _tuple.append((k, sum(v)))
            _all_found_data.append(sum(v))
            print("social media: {}, Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(k,
                                                                                          len(v),
                                                                                          statistics.median(v),
                                                                                          min(v),
                                                                                          max(v),
                                                                                          sum(v)
                                                                                          ))

        _tuple.sort(key=lambda x: x[1], reverse=True)

        print(_tuple)

        print("{}".format(_tuple[0]))
        print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
            len(_tuple),
            statistics.median(_all_found_data),
            min(_all_found_data),
            max(_all_found_data),
            sum(_all_found_data)
        ))

    def create_table_marketplace_posts(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}

        _over_twenty_k = []

        for line in lines:
            line = dict(line)
            if 'prices' in line or 'price' in line:
                if 'prices' in line:
                    _found_seller = line['prices']
                else:
                    _found_seller = line['price']
                if not _found_seller:
                    continue
                if type(_found_seller) is not int and type(_found_seller) is not float:
                    continue
            else:
                continue

            if _found_seller > 20000:
                _over_twenty_k.append(_found_seller)

            if 'social_media' in line:
                _found_platform = line['social_media']
            else:
                continue
            if _found_platform not in _all_platform:
                _all_platform[_found_platform] = [_found_seller]
            else:
                _all_platform[_found_platform] = list(set([_found_seller] + _all_platform[_found_platform]))

        _tuple = []
        _all_found_data = []

        with open("report/graph_data/prices.json", "w") as f_write:
            json.dump(_all_platform, f_write, indent=4)
        for k, v in _all_platform.items():
            _tuple.append((k, sum(v)))
            _all_found_data.append(sum(v))
            print("social media: {}, Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(k,
                                                                                          len(v),
                                                                                          statistics.median(v),
                                                                                          min(v),
                                                                                          max(v),
                                                                                          sum(v)
                                                                                          ))

        _tuple.sort(key=lambda x: x[1], reverse=True)

        print(_tuple)

        print("{}".format(_tuple[0]))
        print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
            len(_tuple),
            statistics.median(_all_found_data),
            min(_all_found_data),
            max(_all_found_data),
            sum(_all_found_data)
        ))

        print(len(_over_twenty_k))

        print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
            len(_over_twenty_k),
            statistics.median(_over_twenty_k),
            min(_over_twenty_k),
            max(_over_twenty_k),
            sum(_over_twenty_k)
        ))

    def create_table_marketplace_key_result(self, _key):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}
        _social_media = {}

        for line in lines:
            line = dict(line)
            _title = line['title']
            if _key in line:
                _found_key = line[_key]
            else:
                _found_key = 'not_provided'

            if _key in line:
                _found_key = line[_key]
                if _found_key and ('Member' in _found_key or 'unknown' in _found_key):
                    continue
                if _found_key and 'Yes' in _found_key:
                    _found_social = line['social_media']
                    if _found_social not in _social_media:
                        _social_media[_found_social] = [_title]
                    else:
                        _social_media[_found_social] = list([_title] + _social_media[_found_social])
            else:
                _found_key = 'unknown'
            if _found_key not in _all_platform:
                _all_platform[_found_key] = [_title]
            else:
                _all_platform[_found_key] = list([_found_key] + _all_platform[_found_key])

        _tuple = []
        _all_found_data = []
        for k, v in _all_platform.items():
            _tuple.append((k, len(v)))
            # if k and 'Yes' in k:
            #     print(v)
            # if type(k) is int:
            _all_found_data.append(len(v))

        _tuple.sort(key=lambda x: x[1], reverse=True)

        print(_tuple)

        print("{}".format(_tuple[0]))
        print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
            len(_tuple),
            statistics.median(_all_found_data),
            min(_all_found_data),
            max(_all_found_data),
            sum(_all_found_data)
        ))

        for k, v in _social_media.items():
            print(k, len(v))

    def create_table_marketplace_seller(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}

        for line in lines:
            line = dict(line)
            if 'seller' in line:
                _found_seller = line['seller']
            elif 'seller_name' in line:
                _found_seller = line['seller_name']
            else:
                _found_seller = 'not_provided'

            if 'platform' in line:
                _found_platform = line['platform']
                if 'AccsMarket' in _found_platform or 'Accsmarket' in _found_platform:
                    _found_platform = 'Accsmarket'
            else:
                _found_platform = 'unknown'
            if _found_platform not in _all_platform:
                _all_platform[_found_platform] = [_found_seller]
            else:
                _all_platform[_found_platform] = list(set([_found_seller] + _all_platform[_found_platform]))

        _tuple = []
        _all_found_data = []
        for k, v in _all_platform.items():
            _tuple.append((k, len(v)))
            _all_found_data.append(len(v))

        _tuple.sort(key=lambda x: x[1], reverse=True)

        print(_tuple)

        print("{}".format(_tuple[0]))
        print("Total: {}, Median:{}, Min:{}, Max:{}, Sum:{}".format(
            len(_tuple),
            statistics.median(_all_found_data),
            min(_all_found_data),
            max(_all_found_data),
            sum(_all_found_data)
        ))

    def create_table_marketplace_url(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_advertised_url = set()
        _all_platform = {}

        for line in lines:
            line = dict(line)
            if 'url' in line:
                _found_url = line['url']
            else:
                _found_url = line['title']

            if 'platform' in line:
                _found_platform = line['platform']
                if 'AccsMarket' in _found_platform or 'Accsmarket' in _found_platform:
                    _found_platform = 'Accsmarket'
            else:
                _found_platform = 'unknown'
            if _found_platform not in _all_platform:
                _all_platform[_found_platform] = [_found_url]
            else:
                _all_platform[_found_platform] = list(set([_found_url] + _all_platform[_found_platform]))

        _tuple = []
        for k, v in _all_platform.items():
            _tuple.append((k, len(v)))

        _tuple.sort(key=lambda x: x[1], reverse=True)

        print(_tuple)

    def create_table_marketplace_social_media(self):
        with open("report/visible_ids/MergedDatabasesWithCategoryMappings.json", "r") as f_read:
            lines = json.load(f_read)
        _all_social_media = {}

        for line in lines:
            line = dict(line)

            if 'social_media' in line:
                _found_social = line['social_media']
                if _found_social == None:
                    print(line)
            else:
                print("line", line)
                _found_social = 'unknown'
            if _found_social not in _all_social_media:
                _all_social_media[_found_social] = 1
            else:
                _all_social_media[_found_social] = 1 + _all_social_media[_found_social]
        print(_all_social_media)


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Data analysis")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    meta_data = Analysis(_arg_value.process_name)
    meta_data.process()
