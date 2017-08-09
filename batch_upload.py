from subprocess import call
import csv
import time
# import argparse

batch_file = 'advertiser_batch.csv'
time_file = 'upload_timing.csv'


def write_cmd(name, ad_id):
    if ultype == 'content' or ultype is None:
        return "python advertiserv6_stable.py --advertiser_id=%s --advertiser_name=%s --update_sandbox=yes" % (ad_id, name)
    elif ultype == 'toc':
        return "python wbnamesv3_stable.py --advertiser_id=%s --advertiser_name=%s --run_tabcmd=yes" % (ad_id, name)
    elif ultype == 'full':
        return "python advertiserv6_stable.py --advertiser_id=%s --advertiser_name=%s --update_sandbox=yes" % (ad_id, name), "python wbnamesv3_stable.py --advertiser_id=%s --advertiser_name=%s --run_tabcmd=yes" % (ad_id, name)
    else:
        ValueError('Enter either "content", "toc", or "full" into col 3 of advertiser_batch.csv')

with open(batch_file, 'rb') as batch, open(time_file, 'ab') as out_file:
    # parser = argparse.ArgumentParser(description='Batch uploads content to Tune BI Tableau Server')
    # parser.add_argument('--type', '-t', action='store', dest='ultype', help='Choices: "content", "toc", or "full" for both content and toc')
    # parser_results = parser.parse_args()
    # ultype = parser_results.ultype
    reader = csv.reader(batch)
    # headers = reader.next()
    writer = csv.writer(out_file)

    for row in reader:

        t_0 = time.time()

        name = row[0]
        ad_id = row[1]
        ultype = row[2]

        if ultype == 'content':
            content = write_cmd(name, ad_id)
            toc = None
            print content
            call(content)

        elif ultype == 'toc':
            toc = write_cmd(name, ad_id)
            content = None
            print toc
            call(toc)

        elif ultype == 'full':
            content, toc = write_cmd(name, ad_id)
            print content
            call(content)
            print toc
            call(toc)

        elif ultype is None:
            print 'Upload type left blank, defaulting to content update only'
            content = write_cmd(name, ad_id)
            toc = None
            print content
            call(content)

        t_1 = time.time()

        t = t_1 - t_0

        print t

        timing_row = [name, ad_id, t]

        writer.writerow(timing_row)

        time.sleep(5)
