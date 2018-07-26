#!/usr/bin/env python2.7
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the
# License. A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
#
# ---------------------------------------------------------------------------------------------------------------------
# get_spot_duration.py uses AWS CLI tools to obtain price history for the last week (by default),
# and prints time duration since the last time Spot price exceeded the bid price.
#
# We use CLI for simplicity and demonstration purposes. For production code please use SDK/boto3
# Input: product-description, region, and combination of instance types and Spot bids prices for each instance type.
# Example:
# $ python get_spot_duration.py -r us-east-1 -b c3.large:0.105,c3.xlarge:0.210 \
#    --product-description 'Linux/UNIX (Amazon VPC)'
#
# v0.1
import argparse
from argparse import RawTextHelpFormatter
import calendar
import json
import sys
import datetime
import time
from subprocess import Popen, PIPE

# Call AWS CLI and obtain JSON output, we could've also used tabular or text, but json is easier to parse.
def make_call(cmdline, profile):
    cmd_args = ['aws', '--output', 'json'] + (['--profile', profile] if profile else []) + cmdline
    p = Popen(cmd_args, stdout=PIPE)
    res, _ = p.communicate()
    if p.wait() != 0:
        sys.stderr.write("Failed to execute: " + " ".join(cmd_args))
        sys.exit(1)
    if not res:
        return {}
    return json.loads(res)


def iso_to_unix_time(iso):
    return calendar.timegm(time.strptime(iso, '%Y-%m-%dT%H:%M:%S.%fZ'))

# For each availability zone, return timestamp of the last time Spot price exceeded specified bid price
def get_last_spot_price_exceeding_the_bid(profile, hours, inst_type, region, product, bid):
    now = datetime.datetime.utcfromtimestamp(time.time())
    start_time = now - datetime.timedelta(hours=hours)
    start_time_unix = calendar.timegm(start_time.utctimetuple())

    #: :type: list of SpotPriceHistory
    res = make_call(["ec2", "--region", region,
                     "describe-spot-price-history",
                         "--start-time", start_time.isoformat(),
                     "--end-time", now.isoformat(),
                     "--instance-types", inst_type,
                     "--product-descriptions", product], profile)

    last_times = {}
    for p in res['SpotPriceHistory']:
        cur_ts = iso_to_unix_time(p['Timestamp'])
        cur_az = p['AvailabilityZone']
        old_ts = last_times.get((inst_type, cur_az), None)

        if old_ts is None:
            last_times[(inst_type, cur_az)] = old_ts = start_time_unix

        if float(p['SpotPrice']) > bid and cur_ts > old_ts:
            last_times[(inst_type, cur_az)] = cur_ts

    return last_times

def get_current_region():
    cmd_args = ['aws', 'configure', 'get', 'region']
    p = Popen(cmd_args, stdout=PIPE)
    res, _ = p.communicate()
    if p.wait() != 0:
        sys.stderr.write("Failed to execute: " + " ".join(cmd_args))
        sys.exit(1)
    if not res:
        return {}
    return res.decode('utf-8').strip()

def validate_product_choice(product_choice):
    product_choices = ["Linux/UNIX", "SUSE Linux", "Windows",
                       "Linux/UNIX (Amazon VPC)", "SUSE Linux (Amazon VPC)",
                       "Windows (Amazon VPC)"]

    return product_choice in product_choices


def get_instance_volatility(instance_name, bid, time_span, product_choice, zone):
    region = get_current_region()
    profile = [] #working with only default profiles for now
    print("Request: Instance-Name {} Time Span {} Bid {} Product Choice {} Zone {}".format(instance_name, bid, time_span, product_choice, zone))
    volatility_map = get_last_spot_price_exceeding_the_bid(profile, time_span, instance_name, region, product_choice, bid)
    for key in volatility_map:
        if key[1] == zone:
            return volatility_map[key]
    return None

def check_is_spot_instance(instance_name):
    pass

if __name__ == '__main__':
    print(get_instance_volatility("m1.xlarge", 0.02, 168, "Linux/UNIX", "us-east-1a"))
