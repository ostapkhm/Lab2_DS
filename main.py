import pandas as pd
import urllib.request
from datetime import datetime
import os


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
        20: "Sevastopol'",
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



