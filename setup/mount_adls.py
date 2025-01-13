# Databricks notebook source
# Replace placeholders with actual values
# storageContainer = 'bronze'
# storageAccount = '<your-storage-account-name>'
# bronzeMountPoint = '/mnt/bronze'
# databricksScopeName = 'Azure_KeyVault'
# StorageSAStoken = 'sastokenforstorage1'

# # Retrieve SAS token from Azure Key Vault via Databricks secret scope

# # Mount ADLS Gen2 container to DBFS

# dbutils.fs.mount(
#     source='wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount),
#     mount_point=bronzeMountPoint,
#     extra_configs={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer, storageAccount): dbutils.secrets.get(scope="adls-scope", key=StorageSAStoken)}
# )
# print('Mounted the storage account successfully')
