storage_account_name = "formula1dl"
client_id  = "b9f023a-70cd-44f6-a2d2-fff1d6939edc"
tenant_id  = "65947afd-b0be-440f-b3d5-2ca66af0ef41"
client_secret = "Uzhj3Iy5-egmxUg95~9k5e~t1H62a5Dx~"

configs = {"fs.azure.account.auth.type":"OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{client_id}", 
            "fs.azure.account.oauth2.client.secret": f"{client_secret}",
             "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage-account_name}/{container_name}",
  extra_configs = configs)


