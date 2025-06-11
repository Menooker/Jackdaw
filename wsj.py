from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chromium.options import ChromiumOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = ChromiumOptions()

proxy = "http://127.0.0.1:8010"
options.add_argument("--headless")  # 可选：无头模式（不弹出浏览器窗口）
options.add_argument(f'--proxy-server={proxy}')  # 设置代理
driver = webdriver.Chrome(options=options)

# 打开网页
url = "https://www.wsj.com/"
driver.get(url)
try:
    # 等待最多10秒直到首页的某个关键元素出现
    element = WebDriverWait(driver, 40).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div[crawler css-j6808u evtohk912]"))
    )
    print("页面加载完成")
except Exception as e:
    print("加载超时:", e)

# 获取网页源码
html = driver.page_source


# class="crawler css-j6808u evtohk912"

# 输出或保存网页内容
print(html[:1000])  # 只打印前1000字符

# 可选：保存到文件
with open("wsj_headless.html", "w", encoding="utf-8") as f:
    f.write(html)

# 关闭浏览器
driver.quit()