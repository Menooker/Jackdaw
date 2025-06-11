import undetected_chromedriver as uc
from subprocess import Popen
import time

'''
sudo apt update
sudo apt install -y xvfb python3-pip unzip curl
pip install undetected-chromedriver

wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install -y ./google-chrome-stable_current_amd64.deb
'''

# 启动 Xvfb（显示编号 :99，可换）
xvfb = Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'])
time.sleep(1)  # 给 Xvfb 一点启动时间

import os
os.environ['DISPLAY'] = ':99'  # 设置 DISPLAY 环境变量

# install
# path = uc.install()  # 默认会缓存到 ~/.undetected_chromedriver/
# driver = uc.Chrome(driver_executable_path=path)

options = uc.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
options.add_argument("--disable-blink-features=AutomationControlled")

# 不使用 headless 模式
driver = uc.Chrome(options=options, use_subprocess=True)
driver.get("https://www.bloomberg.com/next-china")
print(driver.title)
time.sleep(5)

html = driver.page_source
with open("bloomberg.html", "w", encoding="utf-8") as f:
    f.write(html)

driver.quit()
xvfb.terminate()  # 关闭虚拟显示