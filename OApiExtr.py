###python 3.6
#!/usr/bin/env python

import ujson
import pandas as pd
from urllib.request import Request
import urllib
import sys
from pick import pick
from urllib.parse import urlsplit
import flattener

class jsonScraper:

    def __init__(self, url, selType):
        self.sourceUrl = url
        baseUrl = urlsplit(url)
        self.baseUrl = baseUrl.scheme + '://' + baseUrl.hostname
        self.filename = url.replace('https://', '').replace('http://', '').replace('/', '')
        self.filename = self.filename + '.csv'
        self.nextUrl = None
        self.dfList = []
        if selType == 0:
            self.convertLoop()
        else:
            self.chosen = False
            self.convertLoopSelect()


    def convertLoop(self):
        tempDf = self.converter(self.sourceUrl)
        self.dfList.append(tempDf)
        # print(self.sourceUrl)
        # print(self.nextUrl)
        if self.sourceUrl != self.nextUrl:
            self.sourceUrl = self.nextUrl
            try:
                self.convertLoop()
            except TimeoutError:
                print(self.sourceUrl)
            except KeyboardInterrupt:
                self.finalDf = pd.concat(self.dfList)
                print('files collected')
                self.finalDf.to_csv(str(self.filename), index=False)
        else:
            self.finalDf = pd.concat(self.dfList)
            print('all files collected')
            self.finalDf.to_csv(str(self.filename), index=False)

    def convertLoopSelect(self):
        tempDf = self.converter(self.sourceUrl)

        if self.chosen == False:
            title = 'Please choose the columns you want to include in your csv file (press SPACE to mark, ENTER to continue): '
            coloptions = tempDf.columns
            self.selected = pick(coloptions, title, multi_select=True, min_selection_count=1)

        self.chosen = True
        self.dfList.append(tempDf)
        if self.sourceUrl != self.nextUrl:
            self.sourceUrl = self.nextUrl
            try:
                self.convertLoopSelect()
            except TimeoutError:
                print(self.sourceUrl)
            except KeyboardInterrupt:
                self.finalDf = pd.concat(self.dfList)
                print('files collected')
                col_selection = []
                for col in self.selected:
                    col_selection.append(col[0])

                selected_finalDf = self.finalDf[col_selection]
                selected_finalDf.to_csv(str(self.filename), index=False)
        else:
            self.finalDf = pd.concat(self.dfList)
            print('all files collected')
            col_selection = []
            for col in self.selected:
                col_selection.append(col[0])

            selected_finalDf = self.finalDf[col_selection]
            selected_finalDf.to_csv(str(self.filename), index=False)

    def pageLoad(self, extUrl):
        try:
            with urllib.request.urlopen(extUrl) as url:
                data = ujson.loads(url.read().decode())
                self.nextUrl = data['next']
                return data['items']
        except (urllib.error.HTTPError):

            req = Request(extUrl, headers={'User-Agent': 'Mozilla/5.0'})
            try:
                url = urllib.request.urlopen(req)
                data = ujson.loads(url.read().decode())
                self.nextUrl = data['next']
                return data['items']
            except (urllib.error.HTTPError):
                # self.finalDf = pd.concat(self.dfList)
                # print('files collected')
                # self.finalDf.to_csv(str(self.filename), index=False)
                print('HTTP error')

        except ValueError as e:
            extUrl = self.baseUrl + extUrl

            try:
                with urllib.request.urlopen(extUrl) as url:
                    data = ujson.loads(url.read().decode())
                    self.nextUrl = data['next']
                    return data['items']
            except (urllib.error.HTTPError):
                # time.sleep(120)
                req = Request(extUrl, headers={'User-Agent': 'Mozilla/5.0'})
                url = urllib.request.urlopen(req)
                data = ujson.loads(url.read().decode())
                self.nextUrl = data['next']
                return data['items']


    def converter(self, myUrl):
        extJson = self.pageLoad(myUrl)
        newJson = []
        # counter = 0
        if extJson is not None:
            for i in extJson:
                # print(i.keys())
                if 'data' in i.keys():
                    data = flattener.splitObj(i['data'])
                    if data[0] is not None and data[2] is None:
                        data = data[0]
                    elif data[0] is not None and data[2] is not None:
                        data_plus = flattener.splitObj(data[2][0], prefix=data[1])

                        data = data[0]
                        if data_plus[0] is not None:
                            data = {**data, **data_plus[0]}
                        # counter = 0
                    else:
                        #we go down another level
                        data = flattener.splitObj(data[2][0], prefix=data[1])

                        if data[0] is not None and data[2] is None:
                            data = data[0]
                        elif data[0] is not None and data[2] is not None:
                            data_plus = flattener.splitObj(data[2][0], prefix=data[1])

                            data = data[0]
                            if data_plus[0] is not None:
                                data = {**data, **data_plus[0]}
                            else:
                                data = flattener.splitObj(data[2][0], prefix=data[1])

                                if data[0] is not None and data[2] is None:
                                    data = data[0]
                                elif data[0] is not None and data[2] is not None:
                                    data_plus = flattener.splitObj(data[2][0], prefix=data[1])

                                    data = data[0]
                                    if data_plus[0] is not None:
                                        data = {**data, **data_plus[0]}

                    i.pop('data', None)
                    try:
                        j = {**i, **data}
                        newJson.append(j)
                    except TypeError:
                        data = flattener.splitObj(data[2][0], prefix=data[1])

                        if data[0] is not None and data[2] is None:
                            data = data[0]
                        elif data[0] is not None and data[2] is not None:
                            data_plus = flattener.splitObj(data[2][0], prefix=data[1])

                            data = data[0]
                            if data_plus[0] is not None:
                                data = {**data, **data_plus[0]}
                            else:
                                data = flattener.splitObj(data[2][0], prefix=data[1])

                                if data[0] is not None and data[2] is None:
                                    data = data[0]
                                elif data[0] is not None and data[2] is not None:
                                    data_plus = flattener.splitObj(data[2][0], prefix=data[1])

                                    data = data[0]
                                    if data_plus[0] is not None:
                                        data = {**data, **data_plus[0]}

                                try:
                                    j = {**i, **data}
                                    newJson.append(j)
                                except TypeError:
                                    print(i, data)
                                    continue

            apiDF = pd.DataFrame(newJson)
            return apiDF
        else:
            apiDF = pd.DataFrame()
            return apiDF


def main():
    # my code here
    jsonUrl = sys.argv[1]
    title = 'Please choose your favourite extraction mode: '
    options = ['all variables', 'selected variables']
    option, index = pick(options, title)
    x = jsonScraper(jsonUrl, index)

if __name__ == "__main__":
    main()