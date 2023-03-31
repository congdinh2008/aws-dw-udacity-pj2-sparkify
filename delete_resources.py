import configparser

from create_resources import create_clients

def delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    try:
        print("\n Delete cluster:")
        res = redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print("\N Exception deleting cluster: {}".format(e))
    return res['Cluster']

def check_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    try:
        print("\n Check cluster:")
        res = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )
    except Exception as e:
        if "ClusterNotFound" in str(e):
            print("\nCluster deleted!")
        else:
            print("\N Exception checking cluster: {}".format(e))
    else:
        print("\nCluster is deleting!")

def delete_iam_role(iam, IAM_ROLE_NAME):
    try:
        print("\n Detach iam policy:")
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print("\m Delete iam role:")
        iam.delete_role(RoleName=IAM_ROLE_NAME)
    except Exception as e:
        print("\N Exception deleting role: {}".format(e))   

def delete_security_group(ec2, IAM_SECURITY_GROUP):
    try:
        print("\n Delete security group:")
        ec2.delete_security_group(GroupId=IAM_SECURITY_GROUP)
    except Exception as e:
        print("\N Exception deleting security group: {}".format(e))

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    AWS_ACCESS_KEY          = config.get('AWS','KEY')
    AWS_SECRET_ACCESS_KEY   = config.get('AWS','SECRET')

    DWH_REGION              = config.get("CLUSTER","DWH_REGION")
    DWH_CLUSTER_IDENTIFIER  = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    IAM_ROLE_NAME       = config.get("IAM_ROLE", "IAM_ROLE_NAME")

    ec2, s3, iam, redshift = create_clients(DWH_REGION, AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
    
    cluster = delete_cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    
    check_cluster(redshift, DWH_CLUSTER_IDENTIFIER)

    delete_iam_role(iam, IAM_ROLE_NAME)

if __name__ == '__main___':
    main()