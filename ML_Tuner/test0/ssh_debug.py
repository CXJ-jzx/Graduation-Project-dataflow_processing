"""ssh_debug.py"""
import paramiko
import time

HOST = "192.168.56.151"
USER = "root"
PASSWORD = "123456"

print(f"连接 {USER}@{HOST} ...")

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    t0 = time.time()
    client.connect(
        hostname=HOST, port=22, username=USER, password=PASSWORD,
        timeout=10, banner_timeout=10, auth_timeout=10,
        look_for_keys=False,
        allow_agent=False,
    )
    print(f"连接成功! ({time.time()-t0:.1f}s)")

    stdin, stdout, stderr = client.exec_command("echo SSH_OK && hostname")
    print(f"输出: {stdout.read().decode().strip()}")
    print("✅ SSH 正常!")

except Exception as e:
    print(f"❌ 失败: {type(e).__name__}: {e}")
finally:
    client.close()
