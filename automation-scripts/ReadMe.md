## Summary:

The Terraform code will perform the following tasks:
#### 1- Provision a Composer instance - a managed Airflow instance (can take up to 25 minutes)
#### 2- Copy Dataflow Dag to the Composer Bucket (the one used to start Dataflow pipelines)
#### 3- Copy BigQuery Dag to the Composer Bucket (the one used to consolidate temp tables)
#### 4- Creates a GCS bucket (to be used by Dataflow pipelines for: 1-As staging area 2-Dataflow Templates storage)
#### 5- Creates a BigQuery dataset
#### 6- Creates a "DEAD_LETTER_EVENTS" table in BigQuery dataset with the provided schema file
#### 7- Creates a PubSub topic to publish events to it
#### 8- Creates a PubSub subscription to be used by Dataflow mutation pipeline
#### 9- Compiles the Java code for the mutation pipeline and copies the compiled template to a GCS bucket (step 4)
#### 10- Compiles the Java code for the import pipeline and copies the compiled template to a GCS bucket (step 4)
#### 11- Creates composer variables to be used by the Dataflow and BigQuery dags
#### 12- Creates a BigTable instance with a cluster of 3 nodes (the number of nodes is configurable)
#### 13- Creates a BigTable table

That's it!


## Prepare your terraform control box (either on cloud or on premise):
The current implementation stores the Terraform state locally, you might need to change this in production environments and store it remotely (eg: GCS) or version control system.

### 1- Install Homebrew (or use a package manager of your choice):
The below snippit is for installing Homebrew:

```bash
sh -c "$(curl -fsSL https://raw.githubusercontent.com/Linuxbrew/install/master/install.sh)"
test -d ~/.linuxbrew && eval $(~/.linuxbrew/bin/brew shellenv)
test -d /home/linuxbrew/.linuxbrew && eval $(/home/linuxbrew/.linuxbrew/bin/brew shellenv)
test -r ~/.bash_profile && echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.bash_profile
echo "eval \$($(brew --prefix)/bin/brew shellenv)" >>~/.profile
```

### 2- Install Gradle:

To install Gradle from Homebrew:

```bash
brew install gradle
```

To install Gradle from APT:

```bash
apt-get install gradle
```

### 3- Install Terraform and add it PATH:

Run the following:
```bash
wget https://releases.hashicorp.com/terraform/0.11.13/terraform_0.11.13_linux_amd64.zip
unzip terraform*
sudo mv terraform /usr/local/bin/
rm terraform* 
```


## Running Terraform

#### 1- Create a service-account on GCP and download a copy to the "automation-scripts" folder

#### 2- Edit your DAG files for customization:
In bigquery_dag.py:
BUFFER_SECONDS = 5,400 means that the dag waits for 90 minutes from the last modification time of the temp table before processing its contents)

#### 3- Edit **terraform.tfvars** and change the values to match your environment: 

Example:

```bash
gcpCred = "<the-path-to-your-service-account>"
gcpProject = "<your-gcp-project>"
gcpRegion = "<your-gcp-region>" // Default -> us-west1
gcpZone = "<your-gcp-zone>" // Default -> us-west1-a
composerName = "<composer-name>"
pubsubTopic01 = "<pubsub-topic-name>"
bucket01 = "<bucket_name>"
bucketLocation = "<bucket-location>"
gitRepo01 = "<your-git-repo>"
localFile01 = "gcs_bucket.txt"
bigQueryDataset = "<your-bigquery-dataset>"
-- output truncated --
```

#### 4- Run terraform:

To provision: 
```bash
terraform init
terraform plan
terraform apply
```

#### 4- Clean up:

To rollback and cleanup:
```bash
terraform destroy
```

#### 5- To delete specific targets or resources:

The below sample code will delete the bucket resource without confirmation:
```bash
terraform destroy -target google_storage_bucket.bucket_dataflow_templates -auto-approve 
```
