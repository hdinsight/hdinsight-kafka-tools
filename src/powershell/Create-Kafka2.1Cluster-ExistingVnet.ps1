# This script is used to create a Hdinsight Kafka cluster. The script creates a resourge group, WASB storage account, storage accoun in an existing virtual network.
# If these resources already exist, you can comment out the corresponding lines that create new resources. 

# Select the subscription to use
$subscriptionID = "<SubscriptionName>"

# Cluster details
$hdiversion = "4.0"
$token ="<SpecifyAnUniqueString>"
# Resource Group Name
$resourceGroupName = $token + "rg"
# Location
$location = "West US"
# Cluster Dns Name
$clusterName = $token
# Default WASB storage account associated with the cluster
$defaultStorageAccountName = $token + "store"
# Default container Name
$defaultStorageContainerName = $token + "container"
# Number of worker nodes (brokers) in the clusters
$clusterNodes = 1
# Existing virtual network's Id. Format:
# "/subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/RESOURCE_GROUP_NAME/providers/microsoft.network/virtualNetworks/VIRTUAL_NETWORK_NAME",
$virtualNetworkId = "VIRTUAL_NETWORK_ID"
# Subnet Name. Format:
# "/subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/RESOURCE_GROUP_NAME/providers/microsoft.network/virtualNetworks/VIRTUAL_NETWORK_NAME/subnets/SUBNET_NAME"
$subnetName = "SUBNET_NAME"

$clusterCredential = Get-Credential -Message "Enter Cluster user credentials" -UserName "admin"
$clusterSshCredential = Get-Credential -Message "Enter SSH user credentials"

# Sign in to Azure
Login-AzureRmAccount

Select-AzureRmSubscription -SubscriptionId $subscriptionID

# Create an Azure Resource Group
New-AzureRmResourceGroup -Name $resourceGroupName -Location $location

# Create an Azure Storage account and container used as the default storage
New-AzureRmStorageAccount `
    -ResourceGroupName $resourceGroupName `
    -StorageAccountName $defaultStorageAccountName `
    -Location $location `
    -Type Standard_LRS
$defaultStorageAccountKey = (Get-AzureRmStorageAccountKey -ResourceGroupName $resourceGroupName -Name $defaultStorageAccountName)[0].Value
$destContext = New-AzureStorageContext -StorageAccountName $defaultStorageAccountName -StorageAccountKey $defaultStorageAccountKey
New-AzureStorageContainer -Name $defaultStorageContainerName -Context $destContext

# The location of the HDInsight cluster must be in the same data center as the Storage account.
$location = Get-AzureRmStorageAccount -ResourceGroupName $resourceGroupName -StorageAccountName $defaultStorageAccountName | %{$_.Location}

New-AzureRmHDInsightCluster `
    -ClusterName $clusterName `
    -ResourceGroupName $resourceGroupName `
    -HttpCredential $clusterCredential `
    -SshCredential $clusterSshCredential `
    -Location $location `
    -DefaultStorageAccountName "$defaultStorageAccountName.blob.core.windows.net" `
    -DefaultStorageAccountKey $defaultStorageAccountKey `
    -DefaultStorageContainer $defaultStorageContainerName  `
    -ClusterSizeInNodes $clusterNodes `
    -ClusterType Kafka `
    -OSType Linux `
    -Version $hdiversion `
    -HeadNodeSize "Standard_D3"  `
    -WorkerNodeSize "Standard_D3" `
    -VirtualNetworkId $virtualNetworkId `
    -SubnetName $subnetName