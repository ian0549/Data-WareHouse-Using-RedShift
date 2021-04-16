import configparser
import boto3
import json
import pandas as pd


def creat_clients(AWS_REGION,KEY,SECRET):
    """Create clients for the DWH:
       EC2,S3,IAM, and RedShift
       Args: AWS_REGION
             KEY
             SECRET
       Returns: cleints ec2,s3,iam,redshift
       
    """

    ec2 = boto3.resource('ec2',region_name = AWS_REGION,
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)
    
    s3 = boto3.resource('s3',
                       region_name=AWS_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

    iam = boto3.client('iam',region_name = AWS_REGION,
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)

    redshift = boto3.client('redshift',region_name = AWS_REGION,
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)
    
    return ec2, s3, iam, redshift


def create_iam_role(iam,DWH_IAM_ROLE_NAME):
    """Create and attach IAM Role that makes Redshift able to access S3 bucket(ReadOnly)
       Args: IAM cleint,
             DWH IAM Role Name
    """
    # create iam role
    try:
        dwhRole =iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument= json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                           'Effect': 'Allow',
                           'Principal':{'Service':'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
            ))

    except Exception as e:
        print(e)
        
        
def attach_get_role(iam,DWH_IAM_ROLE_NAME):
    """Attach role to iam policy 
       Return: IAM role ARN
    """
    
    #attach policy
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
    #get iam role ARN
    roleArn = iam.get_role(RoleName= DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


def create_redshift_cluster(redshift,
                            DWH_CLUSTER_TYPE,
                            DWH_NODE_TYPE,
                            DWH_NUM_NODES,
                            DWH_DB,
                            DWH_CLUSTER_IDENTIFIER,
                            DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            roleArn):
    """
        Create Redshift cluster
        Args: redshift - redshift client
              DWH_CLUSTER_TYPE - cluster type
              DWH_NODE_TYPE - node type 
              DWH_NUM_NODES - number of nodes
              DWH_DB - Data Warehouse database
              DWH_CLUSTER_IDENTIFIER - Cluster identifier
              DWH_DB_USER - database user
              DWH_DB_PASSWORD - database password
              roleArn - ARN role
              
    """
    try:
        response = redshift.create_cluster(        
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),
            DBName= DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername= DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
    """Get the status of the cluster"""
    
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def delete_cluster(redshift,iam,DWH_CLUSTER_IDENTIFIER,DWH_IAM_ROLE_NAME):
    """Clean up by deleting cluster, to prevent accumulation of cost when not in use"""
    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    
    


def access_cluster_endpoint(ec2,myClusterProps,DWH_PORT):
    """Open an incoming TCP port to access the cluster ednpoint"""
    print(myClusterProps['VpcId'])
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.filter(GroupNames=['default']))[0]
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
    
    
def main():
    
    # load configs
    config = configparser.ConfigParser()
    config.read_file(open('aws_conf.cfg'))

    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    AWS_REGION             = config.get('AWS',"REGION")

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

   

    # ceate cleints
    ec2, s3, iam, redshift = creat_clients(AWS_REGION,KEY,SECRET)

   
    # create role
    create_iam_role(iam,DWH_IAM_ROLE_NAME)
    
    # attach policy and get role arn
    roleArn = attach_get_role(iam,DWH_IAM_ROLE_NAME)
    
    # create cluster
    create_redshift_cluster(redshift,
                            DWH_CLUSTER_TYPE,
                            DWH_NODE_TYPE,
                            DWH_NUM_NODES,
                            DWH_DB,
                            DWH_CLUSTER_IDENTIFIER,
                            DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            roleArn)
    
    # CHECK STATUS OF CLUSTER and RETRIEVE ENDPOINT AND ROLE
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    status = prettyRedshiftProps(myClusterProps)
    print(status)
    
    if status.values[2][1] =="available":

        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


        access_cluster_endpoint(ec2,myClusterProps,DWH_PORT)


    #delete_cluster(redshift,iam,DWH_CLUSTER_IDENTIFIER,DWH_IAM_ROLE_NAME)
    

if __name__ == "__main__":
    main()