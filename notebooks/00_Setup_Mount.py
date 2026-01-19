# 1. Define your storage details (Replace the original one with you own )
storage_account_name = "<YOUR_STORAGE_ACCOUNT_NAME>" 
storage_account_access_key = "<YOUR_STORAGE_ACCOUNT_KEY>" 
container_name = "<YOUR_CONTAINER_NAME>" 

# 2. Configure the mount
mount_point = f"/mnt/{container_name}"
source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
conf = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}

# 3. Check if already mounted, if not, mount it
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = source,
        mount_point = mount_point,
        extra_configs = conf
    )
    print(f"Mounted {container_name} successfully!")
else:
    print(f"{container_name} already mounted.")

