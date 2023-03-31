import boto3
import json
import sys
import psycopg2
import configparser
from time import sleep

#Create clients for IAM, EC2, S3 and Redshift¶
def create_clients(DWH_REGION, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY):
    print("Creating clients for IAM, EC2, S3 and Redshift")
    ec2 = boto3.resource('ec2',
                        region_name = DWH_REGION,
                        aws_access_key_id = AWS_ACCESS_KEY,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
                        )

    s3 = boto3.resource('s3',
                        region_name = DWH_REGION,
                        aws_access_key_id = AWS_ACCESS_KEY,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
                    )

    iam = boto3.client('iam',
                        region_name = DWH_REGION,
                        aws_access_key_id = AWS_ACCESS_KEY,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
                    )

    redshift = boto3.client('redshift',
                        region_name = DWH_REGION,
                        aws_access_key_id = AWS_ACCESS_KEY,
                        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
                        )
    return ec2, s3, iam, redshift

def create_iam_role(iam, IAM_ROLE_NAME):
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path = '/',
            RoleName = IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument = json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )

    except Exception as e:
        print(e)

    try:
        print("1.2 Attaching Policy")

        res = iam.attach_role_policy(RoleName = IAM_ROLE_NAME,
                               PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
        if res!= 200:
            print("\nFailed to attach policy: {}".format(str(res)))
            sys.exit(1)

        print("1.3 Get the IAM role ARN")
        roleArn = iam.get_role(RoleName = IAM_ROLE_NAME)['Role']['Arn']
        print("\n Role ARN: {}".format(str(roleArn)))
        return roleArn

    except Exception as e:
        print("Exception attaching policy: {}".format(str(e)))

def create_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn: str):
    try:
        res = redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print("\nError creating cluster: {}".format(e))
    
    print("Check if cluster is already")
    while True:
        checkResponse = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )
        cluster = checkResponse['Clusters'][0]
        if cluster['ClusterStatus'] == 'available':
            print("Cluster {} available".format(DWH_CLUSTER_IDENTIFIER))
            break
        print("Cluster {} is creating...".format(DWH_CLUSTER_IDENTIFIER))
        sleep(60)
    print("Cluster {} is ready".format(DWH_CLUSTER_IDENTIFIER))
    return cluster

def get_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    while True:
        res = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )
        cluster = res['Clusters'][0]
        if cluster['ClusterStatus'] == 'available':
            print("Cluster {} available".format(DWH_CLUSTER_IDENTIFIER))
            break
        print("Cluster {} is creating...".format(DWH_CLUSTER_IDENTIFIER))
        sleep(60)
    try:
        DWH_ENDPOINT = cluster['Endpoint']['Address']
        IAM_ROLE_ARN = cluster['IamRoles'][0]['IamRoleArn']
        return cluster, DWH_ENDPOINT, IAM_ROLE_ARN
    except Exception as e:
        print("\nError getting cluster: {}".format(e))

def open_tcp_port(ec2, cluster, DWH_PORT):
    try:
        vpc = ec2.Vpc(id = cluster['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    return defaultSg.id

def check_cluster_available(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB):
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    connect = psycopg2.connect(conn_string)
    print(connect.status)
    connect.close()

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    AWS_ACCESS_KEY          = config.get('AWS','AWS_ACCESS_KEY')
    AWS_SECRET_ACCESS_KEY   = config.get('AWS','AWS_SECRET_ACCESS_KEY')

    DWH_REGION              = config.get("CLUSTER","DWH_REGION")
    DWH_CLUSTER_IDENTIFIER  = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_CLUSTER_TYPE        = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES           = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE           = config.get("CLUSTER","DWH_NODE_TYPE")
    IAM_ROLE_NAME           = config.get("IAM_ROLE", "IAM_ROLE_NAME")

    DWH_DB                  = config.get("CLUSTER","DWH_DB")
    DWH_DB_USER             = config.get("CLUSTER","DWH_DB_USER")
    DWH_DB_PASSWORD         = config.get("CLUSTER","DWH_DB_PASSWORD")
    DWH_PORT                = config.get("CLUSTER","DWH_PORT")

    # Cleate client
    ec2, s3, iam, redshift = create_clients(DWH_REGION, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

    roleArn = create_iam_role(iam, IAM_ROLE_NAME)

    create_cluster(redshift, 
                             DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, 
                             DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
    
    cluster, DWH_ENDPOINT, IAM_ROLE_ARN = get_cluster(redshift, DWH_CLUSTER_IDENTIFIER)

    IAM_SECURITY_GROUP = open_tcp_port(ec2, cluster, DWH_PORT)

    config.set("CLUSTER", "DWH_HOST", DWH_ENDPOINT)
    config.set("IAM_ROLE", "IAM_ROLE_ARN", IAM_ROLE_ARN)
    config.set("IAM_ROLE", "IAM_SECURITY_GROUP", IAM_SECURITY_GROUP)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

    check_cluster_available(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)

if __name__ == "__main__":
    main()