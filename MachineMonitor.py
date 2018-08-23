import boto3
from datetime import datetime, timedelta
import logging
from dateutil.tz import tzutc

data_role_arn = "arn:aws:iam::753531232501:role/team-data-role"
data_role_session_name = "team-data-role"


class ec2Monitor:
    def __init__(self, role_arn, role_session_name):
        self.role_arn = role_arn
        self.role_session_name = role_session_name
        self.set_credentials(self.role_arn, self.role_session_name)
        self.set_ec2_client_and_resource()
        self.set_ec2_cloudwatch_client()

    def set_credentials(self, role_arn, role_session_name):
        # use security token service to get credentials from IAM role
        data_ec2_client = boto3.client('sts')
        assumedRoleObject = data_ec2_client.assume_role(
            RoleArn = role_arn,
            RoleSessionName = role_session_name
        )
        self.credentials = assumedRoleObject['Credentials']

    def set_ec2_client_and_resource(self):
        self.ec2_client = boto3.client(
            'ec2',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
        )
        self.ec2_resource = boto3.resource(
            'ec2',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
        )

    def set_ec2_cloudwatch_client(self):
        # Create CloudWatch client
        self.cloudwatch_client = boto3.client(
            'cloudwatch',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
        )

    def list_instances(self):
        response = self.ec2_client.describe_instances()
        instance_list = []
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                # This sample print will output entire Dictionary object
                dict = {}
                # This will print will output the value of the Dictionary key 'InstanceId'
                dict['InstanceId'] = instance['InstanceId']
                dict['State'] = instance['State']
                dict['ImageId'] = instance['ImageId']
                # dict['SecurityGroups'] = instance['SecurityGroups']
                dict['InstanceType'] = instance['InstanceType']
                for tag in instance['Tags']:
                    if (tag['Key'] == 'Name'):
                        dict['Name'] = tag['Value']
                    elif (tag['Key'] == 'App'):
                        dict['App'] = tag['Value']
                    elif (tag['Key'] == 'Owner'):
                        dict['Owner'] = tag['Value']
                instance_list.append(dict)
        return instance_list

    # unit of time_range and period is miniute
    def get_cpu_utilization(self, instance_id,
                            time_range_mins = 360,
                            period_mins = 360):
        # boto3 has limitation on amount of datapoints we can fetch
        time_range_mins = int(time_range_mins)
        period_mins = int(period_mins)
        if (time_range_mins / period_mins > 1000):
            raise Exception('Too many timestamps to track, please reduce time_range or increase period.')
        else:
            # get latest time_range_mins cpu utilization data
            now = datetime.now(tzutc())
            lookback = timedelta(minutes = time_range_mins)
            time_start = now - lookback
            stats = self.cloudwatch_client.get_metric_statistics(
                Namespace = 'AWS/EC2',
                MetricName = 'CPUUtilization',
                StartTime = time_start,
                EndTime = now,
                Statistics = ['Average', 'Maximum'],
                Period = period_mins * 60,
                Dimensions=[
                    {
                        'Name': 'InstanceId',
                        'Value': instance_id
                    },
                ],
            )
            # print(stats)
            return stats

    # determine the sort of result instance
    def instance_sort(self, instance):
        return instance['InstanceType']

    # unit of threshold is percentage
    def find_candidates(self, threshold_percentage = 1):
        logging.info("Finding idle instances")
        # get all instances with necessary attributes
        instances = self.list_instances()
        candidates = []
        for instance in instances:
            metric = self.get_cpu_utilization(instance['InstanceId'])
            # Find candidates
            if metric['Datapoints'] and instance['State']['Name'] == 'running':
                # print(instance)
                is_candidate = True
                for datapoint in metric['Datapoints']:
                    average = datapoint['Average']
                    max = datapoint['Maximum']

                    if average < threshold_percentage: # and max < threshold_percentage:
                        continue
                    else:
                        is_candidate = False
                        break
                if (is_candidate):
                    candidates.append(instance)
            else:
                continue
        candidates.sort(key = self.instance_sort)
        return candidates


# Monitor to watch s3 bucket
class s3Monitor:
    def __init__(self, role_arn, role_session_name):
        self.role_arn = role_arn
        self.role_session_name = role_session_name
        self.set_credentials(self.role_arn, self.role_session_name)
        self.set_s3_client_and_resource()
        # self.set_s3_cloudwatch_client()

    def set_credentials(self, role_arn, role_session_name):
        # use security token service to get credentials from IAM role
        data_sts_client = boto3.client('sts')
        assumedRoleObject = data_sts_client.assume_role(
            RoleArn = role_arn,
            RoleSessionName = role_session_name
        )
        self.credentials = assumedRoleObject['Credentials']

    def set_s3_client_and_resource(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
        )
        self.s3_resource = boto3.resource(
            's3',
            aws_access_key_id = self.credentials['AccessKeyId'],
            aws_secret_access_key = self.credentials['SecretAccessKey'],
            aws_session_token = self.credentials['SessionToken'],
        )

    def get_iterate_objects(self,
                            continuation_token,
                            bucket="data-team.scratch",
                            prefix="qlyu"):
        if (continuation_token):
            query_res = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=continuation_token)
        else:
            query_res = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix)
        objs = query_res['Contents']
        is_truncated = query_res['IsTruncated']
        if (is_truncated): continuation_token = query_res['NextContinuationToken']
        else: continuation_token = None
        return is_truncated, continuation_token, objs

    #threshold to filter out idle directory, unit is day
    def find_candidates(self, bucket, prefix, threshold = 180):
        # get latest time_range_mins cpu utilization data
        now = datetime.now(tzutc())
        lookback = timedelta(days=threshold)
        time_start = now - lookback
        print("time_start: " + str(time_start))
        # store result aggregated by first layer directory
        candidates = {}
        is_truncated = True
        continuation_token = None
        while (is_truncated):
            is_truncated, continuation_token, objs = self.get_iterate_objects(
                continuation_token,
                bucket,
                prefix
            )
            # process the query result
            for obj in objs:
                if (obj['StorageClass'] != 'GLACIER'):
                    # get the first sub directory under prefix
                    # For object under the root prefix directory, we should aggreagte them into one category.
                    split_dir_path = obj['Key'].split("/", 2)
                    if (len(split_dir_path) > 2): first_layer_dir = split_dir_path[1]
                    else: first_layer_dir = "_current_dir"
                    # build the dictionary as value, contains the latest modifiedDate and add-on size
                    temp_dict = candidates.get(first_layer_dir, {'Name': first_layer_dir})
                    temp_dict['LastModified'] = max(
                        temp_dict.get('LastModified', '0000-00-00 00:00:00+00:00'),
                        str(obj['LastModified']))
                    temp_dict['Size'] = temp_dict.get('Size', 0) + obj['Size']
                    candidates[first_layer_dir] = temp_dict
        return [candidate for candidate in candidates.values() if candidate['LastModified'] < str(time_start)]


if __name__ == '__main__':
    ec2Mtr = ec2Monitor(data_role_arn, data_role_session_name)
    s3Mtr = s3Monitor(data_role_arn, data_role_session_name)
    # instance_id = 'i-0e07bb6b1ca57962d'
    res = ec2Mtr.find_candidates()
    for subRes in res:
        print(subRes)
    res = s3Mtr.find_candidates("data-team.scratch", "qlyu", 85)
    for item in res:
        print(item)
