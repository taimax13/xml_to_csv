import urllib.request as libreq

import feedparser
import pandas as pd


class UtilsHdfs:
    def save_to_hdfs(self, data, name):
        return


class HttpConnector:
    def __init__(self, host, api_v, query, max_res):
        self.host=host
        self.api_v=api_v
        self.query=query
        self.max_show=max_res

    def request_data(self):
        url = '{}/{}/query?search_query=all:{}&start=0&max_results={}'.format(self.host, self.api_v, self.query, self.max_show)
        response = libreq.urlopen(url).read()
        feed = feedparser.parse(response)
        framekeyslist = list()
        [framekeyslist.append(i) for i in feed.entries[0].keys()]

        df = pd.DataFrame(columns=[val for val in framekeyslist])
        for item in framekeyslist:
            try:
                df[item] = [post[item] for post in feed.entries]
            except:
                print("Key error %s",item)

        #df.to_csv('./processed_data.csv', index=False)
        #todo hive to parquet


def main():
    host = "http://export.arxiv.org/"
    api_v = "api"
    query_word = "proton";
    max_res = 100
    http_connect = HttpConnector(host, api_v,query_word, max_res)
    r = http_connect.request_data()
    print(type(r))


if __name__ == "__main__":
    main()
