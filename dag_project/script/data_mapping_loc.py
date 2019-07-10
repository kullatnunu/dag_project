import time
import happybase
import logging
import pandas as pd
from tqdm import tqdm
import IP2Location
import multiprocessing
from itertools import product
from geopy.geocoders import Nominatim
from settings.defaults import HBASE_HOST, HBASE_HOST_BACKUP, HBASE_PORT, HBASE_PORT_BACKUP, STAGE_HBASE_HOST, STAGE_HBASE_HOST_BACKUP
logging.basicConfig(level=logging.INFO)

table_name = "table"
table_name2 = "table2"
table_name_update = "update_table"
ip2location_file = 'IPV6.BIN'
out_path = r'/tmp/'+table_name+'.csv'

ym = f'_2019-05-'
# load_hbase＿hours = ["T%.2d" % tk for tk in range(0, 25)]
load_hbase＿hbase_key = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
load_hbase＿hours = ["T%.2d" % tk for tk in range(0, 25)]
down_pool_day_key = ["%.2d" % i for i in range(1, 32)]

IP2LocObj = IP2Location.IP2Location();

table_columns = [b'footprint:creation_time', b'footprint:url_hostname', b'footprint:page_id', b'footprint:fp',
                             b'footprint:session_id', b'footprint:ip', b'footprint:hbase_rowkey']
table2_column = [b'profile_oath:session_id', b'profile_oath:user_gender', b'profile_oath:user_age']


def get_ip2locationrecord_dict(self):
    """
        This function creates a dictionary and assigns
        the keys of the dictionary with the objects of
        the class and returns it in a dictionary format.
    """
    data = {
        b'footprint:country_short': self.country_short,
        b'footprint:country_long': self.country_long,
        b'footprint:region': self.region,
        b'footprint:city': self.city,
        # 'isp': self.isp,
        b'footprint:latitude': self.latitude,
        b'footprint:longitude': self.longitude,
        # b'footprint:domain': self.domain,
        b'footprint:zipcode': self.zipcode,
        b'footprint:timezone': self.timezone,
        # 'netspeed': self.netspeed,
        b'footprint:idd_code': self.idd_code,
        b'footprint:area_code': self.area_code,
        # 'weather_code': self.weather_code,
        # 'weather_name': self.weather_name,
        # 'mcc': self.mcc,
        # 'mnc': self.mnc,
        # 'mobile_brand': self.mobile_brand,
        # 'elevation': self.elevation,
        # 'usage_type': self.usage_type
    }
    for i, j in data.items():
        if type(j) is not bytes:
            data[i] = str(j).encode('utf-8')
    return data


def load_hbase(day_key):
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        logging.info("connect to hbase")
    except Exception:
        connection = happybase.Connection(HBASE_HOST_BACKUP, HBASE_PORT_BACKUP)
        logging.info("connect to hbase_backup")

    table = connection.table(table_name)
    oath_table = connection.table(table_name2)

    list_day = [i + ym + day_key + hour for i in load_hbase＿hbase_key for hour in
                load_hbase＿hours]

    profile_oath_list_day = [i + ym + day_key + hour for i in profile_oath_load_hbase＿hbase_key
                             for hour in load_hbase＿hours]

    profile_oath_hbase_data = {}
    for number, prefix_list in enumerate(profile_oath_list_day):
        prefix = prefix_list
        logging.info(f'prefix = {prefix}')
        profile_scanner = oath_table.scan(row_prefix=prefix.encode(), columns=profile_oath_column, batch_size=1000)

        for key, data in tqdm(profile_scanner, mininterval=10):
            try:
                profile_oath_hbase_data[data[b'profile_oath:session_id']] = {
                    b'profile_oath:user_age': data[b'profile_oath:user_age'],
                    b'profile_oath:user_gender': data[b'profile_oath:user_gender'],}
            except:
                pass

    key_count = 0
    for number, prefix_list in enumerate(list_day):
        prefix = prefix_list
        logging.info(f'prefix = {prefix}')

        scanner = table.scan(row_prefix=prefix.encode(), columns=footprint_columns, batch_size=1000)

        scan_num = 0
        hbase_data = []

        for key, data in tqdm(scanner, mininterval=10):
            IP2LocObj.open(ip2location_file);

            try:
                data[b'footprint:user_gender'] = profile_oath_hbase_data[data[b'footprint:session_id']][
                    b'profile_oath:user_gender']
                data[b'footprint:user_age'] = profile_oath_hbase_data[data[b'footprint:session_id']][
                    b'profile_oath:user_age']
            except:
                pass
            # data_in = dict((k.decode('utf8').split(":")[1], v.decode('utf8')) for k, v in data.items())
            data_in = dict((k, v) for k, v in data.items())
            res = get_ip2locationrecord_dict(IP2LocObj.get_all(data_in[b'footprint:ip'].decode('utf8')))
            data_in.update(res)
            hbase_data.append(data_in)
            scan_num += 1

            ''' need address '''
            # latlon_to_geolocator(data_in)

        ''' save to csv '''
        # save_data(hbase_data,key_count)
        # key_count += 1

        ''' update to hbase '''
        # update_hbase(hbase_data)

        print(f"{prefix} has {scan_num} data")


def latlon_to_geolocator(data_in):
    geolocator = Nominatim()
    lat = IP2LocObj.get_all(data_in['ip']).latitude
    long = IP2LocObj.get_all(data_in['ip']).longitude
    location = geolocator.reverse(f"{lat}, {long}")
    print(location)


def save_data(hbase_data,key_count):
    hbase_df = pd.DataFrame.from_dict(hbase_data)
    if key_count != 0:
        with open(out_path, "a") as writer:
            hbase_df.to_csv(writer, header=False)
    elif key_count == 0:
        with open(out_path, "w") as writer:
            hbase_df.to_csv(writer, header=True)


def update_hbase(hbase_data):
    try:
        connection_update = happybase.Connection(STAGE_HBASE_HOST, HBASE_PORT)
        logging.info("connect to stage_hbase")
    except Exception:
        connection_update = happybase.Connection(STAGE_HBASE_HOST_BACKUP, HBASE_PORT_BACKUP)
        logging.info("connect to stage_hbase_backup")
    table_update = connection_update.table(table_name_update)

    with table_update.batch(batch_size=10000) as bat:
        for update_data in tqdm(hbase_data, mininterval=10):
            row_key_update = update_data[b'footprint:hbase_rowkey']
            bat.put(row_key_update, update_data)


def create_table(table_name_update):
    try:
        connection_update = happybase.Connection(STAGE_HBASE_HOST, HBASE_PORT)
        logging.info("connect to stage_hbase")
    except Exception:
        connection_update = happybase.Connection(STAGE_HBASE_HOST_BACKUP, HBASE_PORT_BACKUP)
        logging.info("connect to stage_hbase_backup")

    families = {
        'footprint': dict()
    }
    connection_update.create_table(table_name_update, families)


def delete_table(table_name_update):
    try:
        connection_update = happybase.Connection(STAGE_HBASE_HOST, HBASE_PORT)
        logging.info("connect to stage_hbase")
    except Exception:
        connection_update = happybase.Connection(STAGE_HBASE_HOST_BACKUP, HBASE_PORT_BACKUP)
        logging.info("connect to stage_hbase_backup")

    connection_update.delete_table(table_name_update, disable=False)


def main():
    hbase_time = time.time()
    # load_hbase(down_pool_day_key)

    p = multiprocessing.Pool()
    p.starmap(load_hbase, product(down_pool_day_key))

    print("--- \nhbase scan successfully in %.2d minutes %.5s seconds ---" % ((time.time() - hbase_time) / 60,
                                                                              (time.time() - hbase_time) % 60))


if __name__ == "__main__":
    main()
