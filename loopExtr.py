###python 3.6
#!/usr/bin/env python

import ujson
import OApiExtr
import sys

def jsonLoader(filename):
    with open(filename) as f:
        links = ujson.load(f)

    # OAScrap = OApiExtr.jsonScraper

    for key in links['data'].keys():
        print(links['data'][key]['data-url'])
        try:
            OApiExtr.jsonScraper(links['data'][key]['data-url'], 0)
        except AttributeError as e:
            print(e, links['data'][key]['data-url'])
        except ValueError as e:
            print(e, links['data'][key]['data-url'])


def main():
    # my code here
    jsonUrl = sys.argv[1]
    jsonLoader(jsonUrl)

if __name__ == "__main__":
    main()