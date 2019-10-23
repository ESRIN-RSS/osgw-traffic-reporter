import asyncio
import argparse
import datetime
import os
import re
import sys
import xlwt
from operator import itemgetter
from ipwhois import IPWhois, IPDefinedError, WhoisLookupError
from ipwhois.exceptions import HTTPLookupError
import time

REGEX_LOGS = r'localhost_access_log.(\d\d\d\d\-\d\d\-\d\d)'
IPS_TO_IGNORE = ['127.0.0.1', '131.176.']
NET_NAMES_TO_IGNORE = ['GOOGLE', 'BAIDU', 'MSFT', 'MICROSOFT-GLOBAL-NET', 'MSFT-GFS', 'CHINANET-IDC-BJ', 'SADF', 'MICROSOFT', 'MICROSOFT-1BLK']


def ipInfo(addr=''):
    from urllib.request import urlopen
    from json import load
    if addr == '':
        url = 'https://ipinfo.io/json'
    else:
        url = 'https://ipinfo.io/' + addr + '/json'
    res = urlopen(url)
    #response from url(if res==None then check connection)
    data = load(res)
    return data['country']


def ping_ip(ip):
    net_name = None
    for _ in range(30):
        try:
            lookup = IPWhois(ip).lookup_rdap(inc_nir=False)
            net_name = lookup['network']['name']
            if net_name and net_name.upper() in NET_NAMES_TO_IGNORE:
                return None, (None, None)
            else:
                country_code = lookup['asn_country_code']
                print(ip, country_code, net_name)
                return ip, (country_code, net_name)
        except Exception as e: # (IPDefinedError, WhoisLookupError, HTTPLookupError):
            print(f"{e}. Retrying...")
            time.sleep(5)
            continue
    if net_name is None:
        return ip, (ipInfo(ip), '')


async def ping_servers(ip_list):
    loop = asyncio.get_event_loop()
    coroutines = [loop.run_in_executor(None, ping_ip, ip) for ip in ip_list]
    return await asyncio.gather(*coroutines)


def ignore_ip(ip):
    for ip_ignore in IPS_TO_IGNORE:
        if ip.startswith(ip_ignore):
            return True
    return False


if __name__ == '__main__':
    if os.path.dirname(sys.argv[0]):
        os.chdir(os.path.dirname(sys.argv[0]))

    cmd_args = argparse.ArgumentParser(description="Parse OSGW tomcat logs to report on access usage")
    cmd_args.add_argument("logpath",
                          help="Log path for the OSGW tomcat logs (contains localhost_access_log.YYYY-MM-DD logs)")
    cmd_args.add_argument("start", help="Start parsing from this date (inclusive), in format YYYY-MM-DD",
                          type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
    cmd_args.add_argument("stop", help="Stop parsing by this date (exclusive), in format YYYY-MM-DD",
                          type=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'))
    cmd_args.add_argument("--output",
                          help="Name for the output file, in excel format (default is metrics_STARTDATE_STOPDATE.xls)")
    cmd_args.add_argument("-nowhois", action='store_true',
                          help="Do not perform whois on the IPs (no country information)")
    args = cmd_args.parse_args()

    if not os.path.isdir(args.logpath):
        print("Not a valid path for the OSGW tomcat logs.")
        exit(-1)

    if args.start and args.stop and (args.start >= args.stop):
        print("--start date should be sooner than --stop date.")
        exit(-1)

    start_date_str = str(args.start).split()[0]
    stop_date_str = str(args.stop).split()[0]

    if args.output:
        filename = args.output
        if not os.path.splitext(filename)[1]:
            filename += '.xls'
    else:
        filename = "metrics_{}_{}.xls".format(start_date_str.replace('-', ''), stop_date_str.replace('-', ''))

    ips_count = {}
    daily_hits = {}
    # Traverse the log path to find the files to parse
    for log_file in os.listdir(args.logpath):
        regex = re.compile(REGEX_LOGS)
        find_match = regex.match(log_file)
        if find_match:
            log_file_date = datetime.datetime.strptime(find_match.group(1), '%Y-%m-%d')
            if args.start and log_file_date < args.start:
                continue
            if args.stop and log_file_date >= args.stop:
                continue

            with open(os.path.join(args.logpath, log_file), 'r', encoding='utf-8') as f:
                for line in f:
                    # 183.129.160.229 - - [29/Aug/2016:00:38:25 +0200] "GET / HTTP/1.1" 200 11418
                    line_regex = re.compile(r'(\d+.\d+.\d+.\d+).*"GET /opensearch.+?" 200')
                    match = line_regex.match(line)
                    if match:
                        ip = match.group(1)
                        if ignore_ip(ip):
                            continue
                        if ip in ips_count:
                            ips_count[ip] += 1
                        else:
                            ips_count[ip] = 1
                        if find_match.group(1) in daily_hits:
                            daily_hits[find_match.group(1)] += 1
                        else:
                            daily_hits[find_match.group(1)] = 1

    workbook = xlwt.Workbook()
    worksheet = workbook.add_sheet('By IP')
    worksheet.write(0, 0, 'Metrics from {} (inclusive) to {} (exclusive):'.format(start_date_str, stop_date_str))
    worksheet.write(1, 0, 'Total')
    # will be written later

    worksheet.write(2, 0, '#Hits')
    worksheet.write(2, 1, 'IP')
    worksheet.write(2, 2, 'Country')
    worksheet.write(2, 3, 'Network Name')
    row = 3

    print("Pinging", len(ips_count.keys()), "IPs")
    hits_by_country = {}
    count_ip = 0

    if not args.nowhois:
        loop = asyncio.get_event_loop()
        ip_queries = loop.run_until_complete(ping_servers(ips_count.keys()))
        ip_countries = dict(ip_queries)

    for ip, hit_count in sorted(ips_count.items(), key=itemgetter(1), reverse=True):
        if args.nowhois:
            ip_country = ''
            net_name = ''
        else:
            if ip in ip_countries:
                ip_country = ip_countries[ip][0]
                net_name = ip_countries[ip][1]
            else:
                # excluded
                continue
        worksheet.write(row, 0, hit_count)
        worksheet.write(row, 1, ip)
        worksheet.write(row, 2, ip_country)
        worksheet.write(row, 3, net_name)
        count_ip += 1
        if ip_country in hits_by_country:
            hits_by_country[ip_country] += hit_count
        else:
            hits_by_country[ip_country] = hit_count
        row += 1

    total_gets = sum(hits_by_country.values())
    worksheet.write(1, 1, total_gets)
    worksheet2 = workbook.add_sheet('By day')
    worksheet2.write(0, 0, 'Metrics from {} (inclusive) to {} (exclusive):'.format(start_date_str, stop_date_str))
    worksheet2.write(1, 0, 'Total')
    worksheet2.write(1, 1, total_gets)
    worksheet2.write(2, 0, 'Day')
    worksheet2.write(2, 1, '#Hits')
    row = 3

    for day, hit_count in sorted(daily_hits.items()):
        worksheet2.write(row, 0, day)
        worksheet2.write(row, 1, hit_count)
        row += 1

    worksheet3 = workbook.add_sheet('By Country')
    worksheet3.write(0, 0, 'Metrics from {} (inclusive) to {} (exclusive):'.format(start_date_str, stop_date_str))
    worksheet3.write(1, 0, 'Total')
    worksheet3.write(1, 1, total_gets)
    worksheet3.write(2, 0, 'Country')
    worksheet3.write(2, 1, '#Hits')
    row = 3

    countries = 0
    country_hits = {}
    for country, hits in sorted(hits_by_country.items(), key=itemgetter(1), reverse=True):
        worksheet3.write(row, 0, country)
        worksheet3.write(row, 1, hits)
        country_hits[country] = hits
        row += 1
        countries += 1

    workbook.save(filename)
    print("Wrote output to {}".format(filename))
    print(total_gets)
    print(countries)
    print(count_ip)
    print(country_hits)
