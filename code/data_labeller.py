import argparse

import shared_util
from db_util import MongoDBActor
from constants import COLLECTIONS


class labelData:
    def __init__(self, function_name):
        self.function_name = function_name

    def process(self):
        if self.function_name == "label_twitter_posts_cryptoaddress":
            self.label_twitter_posts('cryptoaddress')
        elif self.function_name == "label_facebook_posts_cryptoaddress":
            self.label_facebook_posts('cryptoaddress')
        elif self.function_name == "label_tiktok_posts_cryptoaddress":
            self.label_tiktok_posts('cryptoaddress')
        elif self.function_name == "label_youtube_posts_cryptoaddress":
            self.label_youtube_posts('cryptoaddress')
        elif self.function_name == "label_instagram_posts_cryptoaddress":
            self.label_instagram_posts('cryptoaddress')

        elif self.function_name == "label_twitter_posts_email":
            self.label_twitter_posts('email')
        elif self.function_name == "label_facebook_posts_email":
            self.label_facebook_posts('email')
        elif self.function_name == "label_tiktok_posts_email":
            self.label_tiktok_posts('email')
        elif self.function_name == "label_youtube_posts_email":
            self.label_youtube_posts('email')
        elif self.function_name == "label_instagram_posts_email":
            self.label_instagram_posts('email')

        elif self.function_name == "label_twitter_posts_phone":
            self.label_twitter_posts('phone')
        elif self.function_name == "label_facebook_posts_phone":
            self.label_facebook_posts('phone')
        elif self.function_name == "label_tiktok_posts_phone":
            self.label_tiktok_posts('phone')
        elif self.function_name == "label_youtube_posts_phone":
            self.label_youtube_posts('phone')
        elif self.function_name == "label_instagram_posts_phone":
            self.label_instagram_posts('phone')

        elif self.function_name == "label_twitter_posts_url":
            self.label_twitter_posts('url')
        elif self.function_name == "label_facebook_posts_url":
            self.label_facebook_posts('url')
        elif self.function_name == "label_tiktok_posts_url":
            self.label_tiktok_posts('url')
        elif self.function_name == "label_youtube_posts_url":
            self.label_youtube_posts('url')
        elif self.function_name == "label_instagram_posts_url":
            self.label_instagram_posts('url')

        elif self.function_name == "label_twitter_posts_all":
            self.label_twitter_posts('url')
            self.label_twitter_posts('phone')
            self.label_twitter_posts('email')
            self.label_twitter_posts('cryptoaddress')
        elif self.function_name == "label_facebook_posts_all":
            self.label_facebook_posts('url')
            self.label_facebook_posts('phone')
            self.label_facebook_posts('email')
            self.label_facebook_posts('cryptoaddress')
        elif self.function_name == "label_tiktok_posts_all":
            self.label_tiktok_posts('url')
            self.label_tiktok_posts('email')
            self.label_tiktok_posts('phone')
            self.label_tiktok_posts('cryptoaddress')
        elif self.function_name == "label_youtube_posts_all":
            self.label_youtube_posts('url')
            self.label_youtube_posts('email')
            self.label_youtube_posts('phone')
            self.label_youtube_posts('cryptoaddress')
        elif self.function_name == "label_instagram_posts_all":
            self.label_instagram_posts('url')
            self.label_instagram_posts('email')
            self.label_instagram_posts('phone')
            self.label_instagram_posts('cryptoaddress')

    def label_twitter_posts(self, _label):
        _all_text = set()
        _distinct_author_ids = set(MongoDBActor(COLLECTIONS.TWITTER_TEXT).distinct(
            key="author_id"
        ))
        _len = len(_distinct_author_ids)
        for count, each_id in enumerate(_distinct_author_ids):
            print('Processing {}/{} {}'.format(count, _len, each_id))
            _text = set(MongoDBActor(COLLECTIONS.TWITTER_TEXT).distinct(
                key="text", filter={"author_id": each_id}
            ))
            _all_text = _all_text.union(_text)
        if None in _all_text:
            _all_text.remove(None)

        for cnt, each_text in enumerate(_all_text):
            if _label == "cryptoaddress":
                _found = shared_util.get_crypto_address_from_line(each_text)
            elif _label == "phone":
                _found = shared_util.get_phone_number_from_line(each_text)
            elif _label == "email":
                _found = shared_util.get_email(each_text)
            elif _label == "url":
                _found = shared_util.get_url_from_line(each_text)
            else:
                raise Exception('Unsupported case')
            MongoDBActor(COLLECTIONS.MEGA_TWITTER).find_and_modify(key={'text': each_text},
                                                                   data={_label: _found}
                                                                   )

    def label_facebook_posts(self, _label):
        _all_text = set()
        _distinct_author_ids = set(MongoDBActor(COLLECTIONS.FACEBOOK_POSTS).distinct(
            key="user.id"
        ))
        _len = len(_distinct_author_ids)
        for count, each_id in enumerate(_distinct_author_ids):
            print('Processing {}/{} {}'.format(count, _len, each_id))
            _text = set(MongoDBActor(COLLECTIONS.FACEBOOK_POSTS).distinct(
                key="text", filter={"user.id": each_id}
            ))
            _all_text = _all_text.union(_text)
        if None in _all_text:
            _all_text.remove(None)

        _len = len(_all_text)
        for cnt, each_text in enumerate(_all_text):
            print('Processing {}/{} {}'.format(cnt, _len, each_text))
            if _label == "cryptoaddress":
                _found = shared_util.get_crypto_address_from_line(each_text)
            elif _label == "phone":
                _found = shared_util.get_phone_number_from_line(each_text)
            elif _label == "email":
                _found = shared_util.get_email(each_text)
            elif _label == "url":
                _found = shared_util.get_url_from_line(each_text)
            else:
                raise Exception('Unsupported case')
            MongoDBActor(COLLECTIONS.MEGA_FACEBOOK).find_and_modify(key={'text': each_text},
                                                                    data={_label: _found}
                                                                    )

    def label_instagram_posts(self, _label):
        _all_text = set()
        _distinct_author_ids = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNTS).distinct(
            key="id"
        ))
        _len = len(_distinct_author_ids)
        for count, each_id in enumerate(_distinct_author_ids):
            print('Processing {}/{} {}'.format(count, _len, each_id))
            latest_text = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNTS).distinct(
                key="latestPosts.caption", filter={"id": each_id}
            ))

            top_text = set(MongoDBActor(COLLECTIONS.INSTAGRAM_ACCOUNTS).distinct(
                key="topPosts.caption", filter={"id": each_id}
            ))

            _found_text = latest_text.union(top_text)
            _all_text = _all_text.union(_found_text)

        if None in _all_text:
            _all_text.remove(None)

        _len = len(_all_text)
        for cnt, each_text in enumerate(_all_text):
            print('Processing {}/{} {}'.format(cnt, _len, each_text))
            if _label == "cryptoaddress":
                _found = shared_util.get_crypto_address_from_line(each_text)
            elif _label == "phone":
                _found = shared_util.get_phone_number_from_line(each_text)
            elif _label == "email":
                _found = shared_util.get_email(each_text)
            elif _label == "url":
                _found = shared_util.get_url_from_line(each_text)
            else:
                raise Exception('Unsupported case')
            MongoDBActor(COLLECTIONS.MEGA_INSTAGRAM).find_and_modify(key={'text': each_text},
                                                                     data={_label: _found}
                                                                     )

    def label_youtube_posts(self, _label):
        _all_text = set()
        _distinct_author_ids = set(MongoDBActor(COLLECTIONS.YOU_TUBE_ACCOUNTS).distinct(
            key="url"
        ))
        _len = len(_distinct_author_ids)
        for count, each_id in enumerate(_distinct_author_ids):
            print('Processing {}/{} {}'.format(count, _len, each_id))
            _text = set(MongoDBActor(COLLECTIONS.YOU_TUBE_ACCOUNTS).distinct(
                key="data.channelDescription", filter={"url": each_id}
            ))
            _all_text = _all_text.union(_text)
        if None in _all_text:
            _all_text.remove(None)

        _len = len(_all_text)
        for cnt, each_text in enumerate(_all_text):
            print('Processing {}/{} {}'.format(cnt, _len, each_text))
            if _label == "cryptoaddress":
                _found = shared_util.get_crypto_address_from_line(each_text)
            elif _label == "phone":
                _found = shared_util.get_phone_number_from_line(each_text)
            elif _label == "email":
                _found = shared_util.get_email(each_text)
            elif _label == "url":
                _found = shared_util.get_url_from_line(each_text)
            else:
                raise Exception('Unsupported case')
            MongoDBActor(COLLECTIONS.MEGA_YOUTUBE).find_and_modify(key={'text': each_text},
                                                                   data={_label: _found}
                                                                   )

    def label_tiktok_posts(self, _label):
        _all_text = set()
        _distinct_author_ids = set(MongoDBActor(COLLECTIONS.TIK_TOK_ACCOUNTS).distinct(
            key="id"
        ))
        for count, each_id in enumerate(_distinct_author_ids):
            _text = set(MongoDBActor(COLLECTIONS.TIK_TOK_ACCOUNTS).distinct(
                key="data.text", filter={"id": each_id}
            ))
            _all_text = _all_text.union(_text)
        if None in _all_text:
            _all_text.remove(None)

        _len = len(_all_text)
        for cnt, each_text in enumerate(_all_text):
            print('Processing {}/{} {}'.format(cnt, _len, each_text))
            if _label == "cryptoaddress":
                _found = shared_util.get_crypto_address_from_line(each_text)
            elif _label == "phone":
                _found = shared_util.get_phone_number_from_line(each_text)
            elif _label == "email":
                _found = shared_util.get_email(each_text)
            elif _label == "url":
                _found = shared_util.get_url_from_line(each_text)
            else:
                raise Exception('Unsupported case')
            MongoDBActor(COLLECTIONS.MEGA_TIKTOK).find_and_modify(key={'text': each_text},
                                                                  data={_label: _found}
                                                                  )


if __name__ == "__main__":
    _arg_parser = argparse.ArgumentParser(description="Data labeller")
    _arg_parser.add_argument("-p", "--process_name",
                             action="store",
                             required=True,
                             help="processing function name")

    _arg_value = _arg_parser.parse_args()

    meta_data = labelData(_arg_value.process_name)
    meta_data.process()
