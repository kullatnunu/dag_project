from tqdm import tqdm
import happybase
import time
from settings.defaults import HBASE_HOST, HBASE_PORT, HBASE_HOST_BACKUP, HBASE_PORT_BACKUP
from argparse import ArgumentParser


def delete_row(table_name, list_day):
    try:
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        print("connect to HBASE")
    except Exception:
        connection = happybase.Connection(HBASE_HOST_BACKUP, HBASE_PORT_BACKUP)
        print("connect to HBASE_BACKUP")
    table = connection.table(table_name)
    for index, prefix in enumerate(list_day):
        before_remove_num = 0
        after_remove_num = 0
        print(f'===========================================')
        print(f'|Remove prefix = {table_name} {prefix}|')
        scanner = table.scan(row_prefix=prefix.encode(), batch_size=1000)
        for key, data in tqdm(scanner, mininterval=10):
            before_remove_num += 1
            with table.batch(batch_size=10) as b:
                b.delete(key)

        print(f'|Remove {prefix} Done|')

        last_data = []
        scanner2 = table.scan(row_prefix=prefix.encode(), batch_size=1000)
        for key, data in tqdm(scanner2, mininterval=10):
            last_data.append(key)
            after_remove_num += 1
        print(f'Remove {table_name} {prefix}')
        print(f'Before Remove {before_remove_num} Data')
        print(f'After Remove Has {after_remove_num} Data')
        print(f'===========================================')


def main():
    start_time = time.time()
    table_name = 'table'
    year_key = '_2018'

    parser = ArgumentParser()
    parser.add_argument("-y", "--year", help="year", dest="year_key", required=True)
    parser.add_argument("-t", "--table", help="table name", dest="table_name", required=True)
    parser.add_argument("-sm", "--startmonth", help="start month", dest="start_month", required=True)
    parser.add_argument("-em", "--endmonth", help="end month", dest="end_month", required=True)
    parser.add_argument("-sd", "--startday", help="start day", dest="start_day", required=True)
    parser.add_argument("-ed", "--endday", help="end day", dest="end_day", required=True)
    parser.add_argument("-sh", "--starthour", help="start hour", dest="start_hour", required=True)
    parser.add_argument("-eh", "--endhour", help="end hour", dest="end_hour", required=True)
    parser.add_argument("-smin", "--startminute", help="start minute", dest="start_minute", default=0)
    parser.add_argument("-emin", "--endminute", help="end minute", dest="end_minute", default=6)
    args = parser.parse_args()

    month_key = ["-%.2d" % i for i in range(int(args.start_month), int(args.end_month)+1)]
    day_key = ["-%.2d" % i for i in range(int(args.start_day), int(args.end_day)+1)]
    hour_key = ["T%.2d" % tk for tk in range(int(args.start_hour), int(args.end_hour)+1)]
    minute_key = [":%d" % tk for tk in range(int(args.start_minute), int(args.end_minute)+1)]
    hbase_schema_key = ['a', 'b', 'c', 'd', 'e', 'f', 'g']

    list_day = [i + year_key + month + day + hour + minute for i in hbase_schema_key for month in month_key for day in
                day_key for hour in hour_key for minute in minute_key]

    delete_row(args.table_name, list_day)
    end_time = time.time()
    print("--- \nsummary_dataframe save successfully in %.2d hour %.2d minutes %.5s seconds ---" % ((start_time - end_time) / 3600
                                                                                          , (start_time - end_time) / 60, (start_time - end_time) % 60))


if __name__ == "__main__":
    main()
