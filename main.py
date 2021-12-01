import pandas as pd
import urllib.request
from datetime import datetime
import os

from spyre import server


def create_options():
    res = []
    for year in range(1981, 2022):
        res.append({"label": year, "value": year})
    return res


class NoaaDataVisualization(server.App):
    title = "Noaa Data Visualization"

    inputs = [{"type": 'dropdown',
               "label": 'Area dropdown',
               "options": [{"label": "Cherkasy", "value": "Cherkasy"},
                           {"label": "Chernihiv", "value": "Chernihiv"},
                           {"label": "Chernivtsi", "value": "Chernivtsi"},
                           {"label": "Crimea", "value": "Crimea"},
                           {"label": "Dnipropetrovs'k", "value": "Dnipropetrovs'k"},
                           {"label": "Ivano-Frankivs'k", "value": "Ivano-Frankivs'k"},
                           {"label": "Kharkiv", "value": "Kharkiv"},
                           {"label": "Kherson", "value": "Kherson"},
                           {"label": "Khmel'nyts'kyy", "value": "Khmel'nyts'kyy"},
                           {"label": "Kiev", "value": "Kiev"},
                           {"label": "Kiev City", "value": "Kiev City"},
                           {"label": "Kirovohrad", "value": "Kirovohrad"},
                           {"label": "Luhans'k", "value": "Luhans'k"},
                           {"label": "L'viv", "value": "L'viv"},
                           {"label": "Mykolayiv", "value": "Mykolayiv"},
                           {"label": "Odessa", "value": "Odessa"},
                           {"label": "Poltava", "value": "Poltava"},
                           {"label": "Rivne", "value": "Rivne"},
                           {"label": "Sevastopol", "value": "Sevastopol"},
                           {"label": "Sumy", "value": "Sumy"},
                           {"label": "Ternopil", "value": "Ternopil"},
                           {"label": "Transcarpathia", "value": "Transcarpathia"},
                           {"label": "Vinnytsya", "value": "Vinnytsya"},
                           {"label": "Volyn", "value": "Volyn"},
                           {"label": "Zaporizhzhya", "value": "Zaporizhzhya"},
                           {"label": "Zhytomyr", "value": "Zhytomyr"},
                           {"label": "Donets'k", "value": "Donets'k"}],
               "key": 'ticker1',
               "action_id": "update_data"},

              {"type": 'dropdown',
               "label": 'NOAA data dropdown',
               "options": [{"label": "VCI", "value": "VCI"},
                           {"label": "TCI", "value": "TCI"},
                           {"label": "VHI", "value": "VHI"}],
               "key": 'ticker2',
               "action_id": "update_data"},

              {"type": 'text',
               "key": 'range',
               "label": 'date-ranges: month1-month2',
               "value": '9-10',
               "action_id": 'simple_html_output'},

              {"type": 'dropdown',
               "label": 'year dropdown',
               "options": create_options(),
               "key": "ticker3",
               "action_id": "update_data"}]

    controls = [{"type": "hidden",
                  "id": "update_data"}]

    tabs = ["Plot", "Table"]

    outputs = [{"type": "table",
                "id": "table_id",
                "control_id": "update_data",
                "tab": "Table",
                "on_page_load": True},
               {"type": "plot",
                "id": "plot_id",
                "control_id": "update_data",
                "tab": "Plot"}]

    def getData(self, params):
        # get values from input
        area = params['ticker1']
        range_months = self.getHTML(params)
        year = params['ticker3']

        months_arr_range = parse_to_months(range_months)
        weeks_arr = months_to_weeks(months_arr_range)

        new_df = res_df[(res_df.area == area) &
                        (res_df.week.isin(weeks_arr)) & (res_df.year == int(year))]
        return new_df

    def getPlot(self, params):
        # get values from input
        noaa_data = params['ticker2']
        print(noaa_data)

        df = self.getData(params).set_index('week').drop(['year', 'SMN', 'SMT', 'area'], axis=1)[noaa_data]

        plt_obj = df.plot()
        plt_obj.set_xlabel("week")
        plt_obj.set_title(noaa_data)
        fig = plt_obj.get_figure()
        print(type(fig))
        return fig

    def getHTML(self, params):
        range_months = params['range']
        return range_months


def create_options(df):
    res = []
    for year in df.year:
        res.append({"label": year, "value": year})
    return res


def parse_to_months(string):
    # assume valid input month1 and month2
    # month1 < month2

    for i in range(0, len(string)):
        if string[i] == '-':
            idx = i
            break

    month1 = int(string[0: idx])
    month2 = int(string[idx+1:])
    return month1, month2


def months_to_weeks(months_arr):
    # simplifed model of calendar(only 52 weeks)

    month_to_week = {1:  4,
                     2:  4,
                     3:  5,
                     4:  4,
                     5:  4,
                     6:  5,
                     7:  4,
                     8:  4,
                     9:  5,
                     10: 4,
                     11: 4,
                     12: 5}

    month1, month2 = months_arr

    start = 0
    for i in range(1, month1):
        start += month_to_week[i]

    end = 0
    for i in range(1, month2):
        end += month_to_week[i]

    return [i for i in range(start, end + 1)]

# ---------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------
def clean_folder(path):
    filenames = os.listdir(path)
    for filename in filenames:
        os.remove(path + filename)


def download_data_region(index, path):
    url = "https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/get_TS_admin." \
          "php?country=UKR&provinceID={}&year1=1981&year2=2021&type=Mean".format(index)
    wp = urllib.request.urlopen(url)
    text = wp.read()

    # preprocessing
    start_idx = text.find('<pre>'.encode('utf-8'))
    end_idx = text.find('</pre>'.encode('utf-8'))
    new_text = text[(start_idx + 5):end_idx]

    # create new file with current datetime
    current_datetime = datetime.now().strftime("%d-%m-%Y %H_%M_%S")
    path = (path + current_datetime + ' province.{}.csv').format(index)
    out = open(path, 'wb')

    # save raw data
    out.write(new_text)
    out.close()


def write_to_dataframe(path):
    prev = None

    for filename in os.listdir(path):
        df = pd.read_csv(path + filename, names=['year', 'week', 'SMN', 'SMT', 'VCI', 'TCI', 'VHI'], index_col=False)

        # data cleaning
        new_df = df.drop(df.loc[df['VHI'] == -1].index)

        idx1 = filename.find('.')
        idx2 = filename.find('.', idx1 + 1)
        new_df['area'] = int(filename[idx1 + 1:idx2])

        if prev is None:
            prev = new_df
        else:
            prev = pd.concat([prev, new_df])
    return prev


def replace_indexes(df):
    areas = {
        1: "Cherkasy",
        2: "Chernihiv",
        3: "Chernivtsi",
        4: "Crimea",
        5: "Dnipropetrovs'k",
        6: "Donets'k",
        7: "Ivano-Frankivs'k",
        8: "Kharkiv",
        9: "Kherson",
        10: "Khmel'nyts'kyy",
        11: "Kiev",
        12: "Kiev City",
        13: "Kirovohrad",
        14: "Luhans'k",
        15: "L'viv",
        16: "Mykolayiv",
        17: "Odessa",
        18: "Poltava",
        19: "Rivne",
        20: "Sevastopol",
        21: "Sumy",
        22: "Ternopil'",
        23: "Transcarpathia",
        24: "Vinnytsya",
        25: "Volyn",
        26: "Zaporizhzhya",
        27: "Zhytomyr"
    }

    for i in range(1, 28):
        df['area'].replace({i: areas[i]}, inplace=True)

    return df


def specific_area_vhi_over_year(year, area, df):
    vhi_series = df.loc[(df.area == area) & (df.year == year)]['VHI']
    min_vhi = vhi_series.min()
    max_vhi = vhi_series.max()

    print()
    print(vhi_series.array)
    print('min -> ', min_vhi)
    print('max -> ', max_vhi)
    print()


def specific_area_drought_years(area, df):
    return df.loc[(df.area == area) & (df['VHI'] <= 35)]['year'].unique()


def specific_area_severe_drought_years(area, df):
    return df.loc[(df.area == area) & (df['VHI'] <= 15)]['year'].unique()


def create_dataframe(path):
    clean_folder(path)
    for i in range(1, 28):
        download_data_region(i, path)

    df = write_to_dataframe(path)
    new_df = replace_indexes(df)
    return new_df


path = r'C:\Users\Ostap\PycharmProjects\Lab2\data\\'
res_df = create_dataframe(path)

app = NoaaDataVisualization()
app.launch(port=9093)